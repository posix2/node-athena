import {
  AthenaClient as AwsAthenaClient,
  AthenaServiceException,
  EncryptionOption,
  GetQueryExecutionCommand,
  StartQueryExecutionCommand,
  StartQueryExecutionInput,
  StopQueryExecutionCommand,
  TooManyRequestsException,
} from '@aws-sdk/client-athena'
import { GetObjectCommand, S3 } from '@aws-sdk/client-s3'
import { Readable } from 'stream'

export interface AthenaRequestConfig {
  bucketUri: string
  baseRetryWait?: number
  retryWaitMax?: number
  retryCountMax?: number
  database?: string
  encryptionOption?: EncryptionOption
  encryptionKmsKey?: string
  workGroup?: string
}

const defaultBaseRetryWait = 200
const defaultRetryWaitMax = 10000
const defaultRetryCountMax = 10

export class AthenaRequest {
  private athena: AwsAthenaClient
  private s3: S3
  constructor(athena: any, s3: any) {
    this.athena = athena
    this.s3 = s3
  }
  public startQuery(query: string, config: AthenaRequestConfig) {
    return new Promise<string>((resolve, reject) => {
      let retryCount = 0
      const params: StartQueryExecutionInput = {
        QueryString: query,
        ResultConfiguration: {
          OutputLocation: config.bucketUri,
          ...(config.encryptionOption && {
            EncryptionConfiguration: {
              EncryptionOption: config.encryptionOption,
              ...(config.encryptionKmsKey && {
                KmsKey: config.encryptionKmsKey,
              }),
            },
          }),
        },
        QueryExecutionContext: {
          Database: config.database || 'default',
        },
        WorkGroup: config.workGroup || 'primary',
      }
      const loopFunc = async () => {
        try {
          const data = await this.athena.send(
            new StartQueryExecutionCommand(params),
          )
          return data.QueryExecutionId
        } catch (err) {
          if (isRetryException(err) && canRetry(retryCount, config)) {
            let wait =
              (config.baseRetryWait || defaultBaseRetryWait) *
              Math.pow(2, retryCount++)
            wait = Math.min(wait, config.retryWaitMax || defaultRetryWaitMax)
            return setTimeout(loopFunc, wait)
          }
          throw err
        }
      }
      loopFunc()
    })
  }

  public checkQuery(queryId: string, config: AthenaRequestConfig) {
    return new Promise<boolean>((resolve, reject) => {
      this.getQueryExecution(queryId, config)
        .then((queryExecution: any) => {
          const state = queryExecution.Status.State
          let isSucceed: boolean = false
          let error: Error | null = null
          switch (state) {
            case 'QUEUED':
            case 'RUNNING':
              isSucceed = false
              break
            case 'SUCCEEDED':
              isSucceed = true
              break
            case 'FAILED':
              isSucceed = false
              const errMsg =
                queryExecution.Status.StateChangeReason ||
                'FAILED: Execution Error'
              error = new Error(errMsg)
              break
            case 'CANCELLED':
              isSucceed = false
              error = new Error('FAILED: Query CANCELLED')
              break
            default:
              isSucceed = false
              error = new Error(`FAILED: UnKnown State ${state}`)
          }
          if (error) {
            return reject(error)
          }
          return resolve(isSucceed)
        })
        .catch((err: AthenaServiceException) => {
          return reject(err)
        })
    })
  }

  public stopQuery(queryId: string, config: AthenaRequestConfig) {
    return new Promise<void>((resolve, reject) => {
      let retryCount = 0
      const params = {
        QueryExecutionId: queryId,
      }
      const loopFunc = async () => {
        try {
          await this.athena.send(new StopQueryExecutionCommand(params))
          return
        } catch (err) {
          if (isRetryException(err) && canRetry(retryCount, config)) {
            const wait = Math.pow(
              config.baseRetryWait || defaultBaseRetryWait,
              retryCount++,
            )
            return setTimeout(loopFunc, wait)
          }
          throw err
        }
      }
      loopFunc()
    })
  }

  public getQueryExecution(queryId: string, config: AthenaRequestConfig) {
    return new Promise<any>((resolve, reject) => {
      let retryCount = 0
      const params = {
        QueryExecutionId: queryId,
      }
      const loopFunc = async () => {
        try {
          const data = await this.athena.send(
            new GetQueryExecutionCommand(params),
          )
          return data.QueryExecution
        } catch (err) {
          if (isRetryException(err) && canRetry(retryCount, config)) {
            const wait = Math.pow(
              config.baseRetryWait || defaultBaseRetryWait,
              retryCount++,
            )
            return setTimeout(loopFunc, wait)
          }
          throw err
        }
      }
      loopFunc()
    })
  }

  public async getResultsStream(s3Uri: string): Promise<Readable> {
    const arr = s3Uri.replace('s3://', '').split('/')
    const bucket = arr.shift() || ''
    const key = arr.join('/')
    const response = await this.s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    )
    // TODO retore a proper streaming solution - this reads all the bytes up front before synthesizing a stream.
    const data = await response.Body!.transformToByteArray()
    return Readable.from(data)
  }
}

function isRetryException(err: any) {
  return (
    // err.code === 'ThrottlingException' || // XXX identify the sdk3 analog to this.
    err instanceof TooManyRequestsException ||
    err.message === 'Query exhausted resources at this scale factor' // XXX confirm the sdk3 analog to this.
  )
}

function canRetry(retryCount: number, config: AthenaRequestConfig) {
  return retryCount < (config.retryCountMax || defaultRetryCountMax)
}

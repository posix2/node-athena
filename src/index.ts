import { AthenaClient as AwsAthenaClient } from '@aws-sdk/client-athena'
import { S3 } from '@aws-sdk/client-s3'
import {
  AthenaClient,
  AthenaClientConfig,
  setConcurrentExecMax,
} from './lib/client'
import { AthenaRequest } from './lib/request'

export interface AwsConfig {
  region: string
  accessKeyId?: string
  secretAccessKey?: string
}

export * from './lib/client'

export default class Athena {
  public static createClient = createClient
  public static setConcurrentExecMax = setConcurrentExecMax
}

export function createClient(
  clientConfig: AthenaClientConfig,
  awsConfig: AwsConfig,
) {
  if (
    clientConfig === undefined ||
    clientConfig.bucketUri === undefined ||
    clientConfig.bucketUri.length === 0
  ) {
    throw new Error('bucket uri required')
  }

  if (
    awsConfig === undefined ||
    awsConfig.region === undefined ||
    awsConfig.region.length === 0
  ) {
    throw new Error('region required')
  }

  const athena = new AwsAthenaClient({ ...awsConfig, apiVersion: '2017-05-18' })
  const s3 = new S3({ ...awsConfig, apiVersion: '2006-03-01' })
  const request = new AthenaRequest(athena, s3)
  return new AthenaClient(request, clientConfig)
}

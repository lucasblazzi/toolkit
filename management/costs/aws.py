from datetime import datetime, timezone

import boto3
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError


logger = Logger(child=True)


class AWS:

    def __init__(self, account, role, service, region):
        self.account = account
        self.role = role
        self.service = service
        self.region = region
        self._credentials = None

    @property
    def role_arn(self):
        return f"arn:aws:iam::{self.account}:role/{self.role}"

    @property
    def is_credentials_expired(self):
        expiration = self._credentials["Expiration"]
        return datetime.now(timezone.utc) > expiration

    @property
    def credentials(self):
        if self._credentials is None or self.is_credentials_expired:
            try:
                client = boto3.client("sts")
                assumed_role = client.assume_role(
                    RoleArn=self.role_arn,
                    RoleSessionName="CostAnalyzingSession"
                )
                self._credentials = assumed_role["Credentials"]
            except ClientError as e:
                logger.error(f"Failed to assume role: {e}")
                raise e

            logger.info("Credentials mapped successfully")
        return self._credentials

    @property
    def session(self):
        session = boto3.Session(
            aws_access_key_id=self.credentials["AccessKeyId"],
            aws_secret_access_key=self.credentials["SecretAccessKey"],
            aws_session_token=self.credentials["SessionToken"],
            region_name=self.region
        )
        logger.info("Session instantiated successfully")
        return session

    @property
    def client(self):
        try:
            client = self.session.client(service_name=self.service)
            logger.info(f"Assumed role client instantiated: {self.credentials}")
        except Exception as e:
            logger.error(e)
            client = boto3.client(self.service)

        logger.info("Client built successfully")
        return client

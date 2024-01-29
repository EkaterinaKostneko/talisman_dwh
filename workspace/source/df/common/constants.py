"""
Created on Jan 15, 2021

@author: pymancer@gmail.com
"""
from pathlib import PosixPath

POSTGRES_PORT = 5432
MSSQL_PORT = 1433
ORACLE_PORT = 1521
FIREBIRD_PORT = 3050

WS_ROOT = PosixPath('/app/ws')
CACHE_ROOT = WS_ROOT.joinpath('cache')
SCHEMAS_FOLDER = WS_ROOT.joinpath('source/df/common/schemas/static')
METADATA_FOLDER = WS_ROOT.joinpath('metadata')
SHARE_FOLDER = WS_ROOT.joinpath('share')

MODELS_META = 'models'
RULES_META = 'rules'

SYSTEM_CONNECTION_ENV = 'AF_DATAFLOW_CONNECTION'
SYSTEM_SCHEMA_ENV = 'AF_DATAFLOW_SCHEMA'
DEFAULT_SCHEMA_ENV = 'AF_DWH_DB_SCHEMA'

DC_API_VERSION = '1.0'
DEFAULT_ORACLE_VERSION = '12.1'

UNDEFINED = 'Undefined'

ISOLATION_LEVELS = ("AUTOCOMMIT", "READ COMMITTED", "READ UNCOMMITTED", "REPEATABLE READ", "SERIALIZABLE")

# Исключая 'database': 'Database', 'pwd': 'PWD', 'server': 'Server', 'uid': 'UID',
MSSQL_OPTION_MAPPER = {
    'addr': 'Addr',
    'address': 'Address',
    'ansi_npw': 'AnsiNPW',
    'app': 'APP',
    'application_intent': 'ApplicationIntent',
    'attach_db_file_name': 'AttachDBFileName',
    'authentication': 'Authentication',
    'auto_translate': 'AutoTranslate',
    'column_encryption': 'ColumnEncryption',
    'connect_retry_count': 'ConnectRetryCount',
    'connect_retry_interval': 'ConnectRetryInterval',
    'description': 'Description',
    'driver': 'Driver',
    'dsn': 'DSN',
    'encrypt': 'Encrypt',
    'failover_partner': 'Failover_Partner',
    'failover_partner_spn': 'FailoverPartnerSPN',
    'file_dsn': 'FileDSN',
    'get_data_extensions': 'GetDataExtensions',  # (v18.0+)
    'hostname_in_certificate': 'HostnameInCertificate',  # (v18.0+)
    'ip_address_preference': 'IpAddressPreference',  # (v18.1+)
    'keep_alive': 'KeepAlive',  # (v17.4+; DSN only prior to 17.8)
    'keep_alive_interval': 'KeepAliveInterval',  # (v17.4+; DSN only prior to 17.8)
    'keystore_authentication': 'KeystoreAuthentication',
    'keystore_principal_id': 'KeystorePrincipalId',
    'keystore_secret': 'KeystoreSecret',
    'language': 'Language',
    'long_as_max': 'LongAsMax',  # (v18.0+)
    'mars__connection': 'MARS_Connection',
    'multi_subnet_failover': 'MultiSubnetFailover',
    'net': 'Net',
    'network': 'Network',
    'query_log_on': 'QueryLog_On',
    'query_log_file': 'QueryLogFile',
    'query_log_time': 'QueryLogTIme',
    'quoted_id': 'QuotedId',
    'regional': 'Regional',
    'replication': 'Replication',
    'retry_exec': 'RetryExec',  # (18.1+)
    'save_file': 'SaveFile',
    'server_certificate': 'ServerCertificate',  # (v18.1+)
    'server_spn': 'ServerSPN',
    'stats_log_on': 'StatsLog_On',
    'stats_log_file': 'StatsLogFile',
    'transparent_network_ip_resolution': 'TransparentNetworkIPResolution',
    'trusted_connection': 'Trusted_Connection',
    'trust_server_certificate': 'TrustServerCertificate',
    'use_fmt_only': 'UseFMTONLY',
    'wsid': 'WSID',
    'client_certificate': 'ClientCertificate',
    'client_key': 'ClientKey'
}

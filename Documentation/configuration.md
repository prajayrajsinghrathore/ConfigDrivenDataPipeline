Configuration Reference
This page contains the list of all the available Airflow configurations that you can set in airflow.cfg file or using environment variables.

Use the same configuration across all the Airflow components. While each component does not require all, some configurations need to be same otherwise they would not work as expected. A good example for that is secret_key which should be same on the Webserver and Worker to allow Webserver to fetch logs from Worker.

The webserver key is also used to authorize requests to Celery workers when logs are retrieved. The token generated using the secret key has a short expiry time though - make sure that time on ALL the machines that you run Airflow components on is synchronized (for example using ntpd) otherwise you might get “forbidden” errors when the logs are accessed.

Note

For more information see Setting Configuration Options.

Provider-specific configuration options
Some of the providers have their own configuration options, you will find details of their configuration in the provider’s documentation.

You can find all the provider configuration in configurations specific to providers

Airflow configuration options
Sections:

[api]

[api_auth]

[core]

[dag_processor]

[database]

[email]

[execution_api]

[kerberos]

[lineage]

[logging]

[metrics]

[operators]

[scheduler]

[secrets]

[sensors]

[sentry]

[smtp]

[traces]

[triggerer]

[workers]

[api]
access_control_allow_headers
Added in version 2.1.0.

Used in response to a preflight request to indicate which HTTP headers can be used when making the actual request. This header is the server side response to the browser’s Access-Control-Request-Headers header.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__API__ACCESS_CONTROL_ALLOW_HEADERS

access_control_allow_methods
Added in version 2.1.0.

Specifies the method or methods allowed when accessing the resource.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__API__ACCESS_CONTROL_ALLOW_METHODS

access_control_allow_origins
Added in version 2.2.0.

Indicates whether the response can be shared with requesting code from the given origins. Separate URLs with space.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__API__ACCESS_CONTROL_ALLOW_ORIGINS

auto_refresh_interval
Added in version 2.2.0.

How frequently, in seconds, the DAG data will auto-refresh in graph or grid view when auto-refresh is turned on

Type:
integer

Default:
3

Environment Variable:
AIRFLOW__API__AUTO_REFRESH_INTERVAL

base_url
The base url of the API server. Airflow cannot guess what domain or CNAME you are using. If the Airflow console (the front-end) and the API server are on a different domain, this config should contain the API server endpoint.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API__BASE_URL

Example:
https://my-airflow.company.com

default_wrap
Added in version 1.10.4.

Default setting for wrap toggle on DAG code and TI log views.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__API__DEFAULT_WRAP

enable_swagger_ui
Added in version 2.6.0.

Boolean for running SwaggerUI in the webserver.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__API__ENABLE_SWAGGER_UI

expose_config
Expose the configuration file in the web server. Set to non-sensitive-only to show all values except those that have security implications. True shows all values. False hides the configuration completely.

Type:
string

Default:
False

Environment Variable:
AIRFLOW__API__EXPOSE_CONFIG

expose_stacktrace
Expose stacktrace in the web server

Type:
string

Default:
False

Environment Variable:
AIRFLOW__API__EXPOSE_STACKTRACE

fallback_page_limit
Added in version 2.0.0.

Used to set the default page limit when limit param is zero or not provided in API requests. Otherwise if positive integer is passed in the API requests as limit, the smallest number of user given limit or maximum page limit is taken as limit.

Type:
integer

Default:
50

Environment Variable:
AIRFLOW__API__FALLBACK_PAGE_LIMIT

grid_view_sorting_order
Added in version 2.7.0.

Sorting order in grid view. Valid values are: topological, hierarchical_alphabetical

Type:
string

Default:
topological

Environment Variable:
AIRFLOW__API__GRID_VIEW_SORTING_ORDER

hide_paused_dags_by_default
By default, the webserver shows paused DAGs. Flip this to hide paused DAGs by default

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__API__HIDE_PAUSED_DAGS_BY_DEFAULT

host
The ip specified when starting the api server

Type:
string

Default:
0.0.0.0

Environment Variable:
AIRFLOW__API__HOST

instance_name
Added in version 2.1.0.

Sets a custom homepage heading and site title for all Airflow UI pages. If set, the Dashboard will display this value instead of the default “Welcome” message.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API__INSTANCE_NAME

log_config
Path to the logging configuration file for the uvicorn server. If not set, the default uvicorn logging configuration will be used.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API__LOG_CONFIG

Example:
path/to/logging_config.yaml

log_fetch_timeout_sec
The amount of time (in secs) webserver will wait for initial handshake while fetching logs from other worker machine

Type:
string

Default:
5

Environment Variable:
AIRFLOW__API__LOG_FETCH_TIMEOUT_SEC

maximum_page_limit
Added in version 2.0.0.

Used to set the maximum page limit for API requests. If limit passed as param is greater than maximum page limit, it will be ignored and maximum page limit value will be set as the limit

Type:
integer

Default:
100

Environment Variable:
AIRFLOW__API__MAXIMUM_PAGE_LIMIT

page_size
Consistent page size across all listing views in the UI

Type:
integer

Default:
50

Environment Variable:
AIRFLOW__API__PAGE_SIZE

port
The port on which to run the api server

Type:
string

Default:
8080

Environment Variable:
AIRFLOW__API__PORT

require_confirmation_dag_change
Added in version 2.9.0.

Require confirmation when changing a DAG in the web UI. This is to prevent accidental changes to a DAG that may be running on sensitive environments like production. When set to True, confirmation dialog will be shown when a user tries to Pause/Unpause, Trigger a DAG

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__API__REQUIRE_CONFIRMATION_DAG_CHANGE

secret_key
Secret key used to run your api server. It should be as random as possible. However, when running more than 1 instances of the api, make sure all of them use the same secret_key otherwise one of them will error with “CSRF session token is missing”. The api key is also used to authorize requests to Celery workers when logs are retrieved. The token generated using the secret key has a short expiry time though - make sure that time on ALL the machines that you run airflow components on is synchronized (for example using ntpd) otherwise you might get “forbidden” errors when the logs are accessed.

Type:
string

Default:
{SECRET_KEY}

Environment Variables:
AIRFLOW__API__SECRET_KEY

AIRFLOW__API__SECRET_KEY_CMD

AIRFLOW__API__SECRET_KEY_SECRET

ssl_cert
Paths to the SSL certificate and key for the api server. When both are provided SSL will be enabled. This does not change the api server port. The same SSL certificate will also be loaded into the worker to enable it to be trusted when a self-signed certificate is used.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__API__SSL_CERT

ssl_key
Paths to the SSL certificate and key for the api server. When both are provided SSL will be enabled. This does not change the api server port.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__API__SSL_KEY

worker_timeout
Number of seconds the API server waits before timing out on a worker

Type:
integer

Default:
120

Environment Variable:
AIRFLOW__API__WORKER_TIMEOUT

workers
Number of workers to run on the API server. Should be roughly equal to the number of cpu cores available. If you need to scale the API server, strongly consider deploying multiple API servers instead of increasing the number of workers; See https://github.com/apache/airflow/issues/52270.

Type:
integer

Default:
1

Environment Variable:
AIRFLOW__API__WORKERS

access_logfile (Deprecated)
Deprecated since version 3.1.0: The option has been moved to api.log_config

[api_auth]
Settings relating to authentication on the Airflow APIs

jwt_algorithm
Added in version 3.0.0.

The algorithm name use when generating and validating JWT Task Identities.

This value must be appropriate for the given private key type.

If this is not specified Airflow makes some guesses as what algorithm is best based on the key type.

(“HS512” if jwt_secret is set, otherwise a key-type specific guess)

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API_AUTH__JWT_ALGORITHM

Example:
"EdDSA" or "HS512"

jwt_audience
Added in version 3.0.0.

The audience claim to use when generating and validating JWTs for the API.

This variable can be a single value, or a comma-separated string, in which case the first value is the one that will be used when generating, and the others are accepted at validation time.

Not required, but strongly encouraged.

See also jwt_audience

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API_AUTH__JWT_AUDIENCE

Example:
my-unique-airflow-id

jwt_cli_expiration_time
Added in version 3.0.0.

Number in seconds until the JWTs used for authentication expires for CLI commands. When the token expires, all CLI calls using this token will fail on authentication.

Make sure that time on ALL the machines that you run airflow components on is synchronized (for example using ntpd) otherwise you might get “forbidden” errors.

Type:
integer

Default:
3600

Environment Variable:
AIRFLOW__API_AUTH__JWT_CLI_EXPIRATION_TIME

jwt_expiration_time
Added in version 3.0.0.

Number in seconds until the JWTs used for authentication expires. When the token expires, all API calls using this token will fail on authentication.

Make sure that time on ALL the machines that you run airflow components on is synchronized (for example using ntpd) otherwise you might get “forbidden” errors.

See also jwt_expiration_time

Type:
integer

Default:
86400

Environment Variable:
AIRFLOW__API_AUTH__JWT_EXPIRATION_TIME

jwt_issuer
Added in version 3.0.0.

Issuer of the JWT. This becomes the iss claim of generated tokens, and is validated on incoming requests.

Ideally this should be unique per individual airflow deployment

Not required, but strongly recommended to be set.

See also jwt_audience

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API_AUTH__JWT_ISSUER

Example:
http://my-airflow.mycompany.com

jwt_kid
Added in version 3.0.0.

The Key ID to place in header when generating JWTs. Not used in the validation path.

If this is not specified the RFC7638 thumbprint of the private key will be used.

Ignored when jwt_secret is used.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API_AUTH__JWT_KID

Example:
my-key-id

jwt_leeway
Added in version 3.0.0.

Number of seconds leeway in validating expiry time of JWTs to account for clock skew between client and server

Type:
integer

Default:
10

Environment Variable:
AIRFLOW__API_AUTH__JWT_LEEWAY

jwt_private_key_path
Added in version 3.0.0.

The path to a file containing a PEM-encoded private key use when generating Task Identity tokens in the executor.

Mutually exclusive with jwt_secret.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API_AUTH__JWT_PRIVATE_KEY_PATH

Example:
/path/to/private_key.pem

jwt_secret
Added in version 3.0.0.

Secret key used to encode and decode JWTs to authenticate to public and private APIs.

It should be as random as possible. However, when running more than 1 instances of API services, make sure all of them use the same jwt_secret otherwise calls will fail on authentication.

Mutually exclusive with jwt_private_key_path.

Type:
string

Default:
{JWT_SECRET_KEY}

Environment Variables:
AIRFLOW__API_AUTH__JWT_SECRET

AIRFLOW__API_AUTH__JWT_SECRET_CMD

AIRFLOW__API_AUTH__JWT_SECRET_SECRET

trusted_jwks_url
Added in version 3.0.0.

The public signing keys of Task Execution token issuers to trust. It must contain the public key related to jwt_private_key_path else tasks will be unlikely to execute successfully.

Can be a local file path (without the file:// prefix) or an http or https URL.

If a remote URL is given it will be polled periodically for changes.

Mutually exclusive with jwt_secret.

If a jwt_private_key_path is given but this settings is not set then the private key will be trusted. If this is provided it is your responsibility to ensure that the private key used for generation is in this list.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__API_AUTH__TRUSTED_JWKS_URL

Example:
"/path/to/public-jwks.json" or "https://my-issuer/.well-known/jwks.json"

[core]
allowed_deserialization_classes
Added in version 2.5.0.

Space-separated list of classes that may be imported during deserialization. Items can be glob expressions. Python built-in classes (like dict) are always allowed.

Type:
string

Default:
airflow.*

Environment Variable:
AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES

Example:
airflow.* my_mod.my_other_mod.TheseClasses*

allowed_deserialization_classes_regexp
Added in version 2.8.2.

Space-separated list of classes that may be imported during deserialization. Items are processed as regex expressions. Python built-in classes (like dict) are always allowed. This is a secondary option to [core] allowed_deserialization_classes.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES_REGEXP

asset_manager_class
Added in version 3.0.0.

Class to use as asset manager.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__CORE__ASSET_MANAGER_CLASS

Example:
airflow.assets.manager.AssetManager

asset_manager_kwargs
Added in version 3.0.0.

Kwargs to supply to asset manager.

Type:
string

Default:
None

Environment Variables:
AIRFLOW__CORE__ASSET_MANAGER_KWARGS

AIRFLOW__CORE__ASSET_MANAGER_KWARGS_CMD

AIRFLOW__CORE__ASSET_MANAGER_KWARGS_SECRET

Example:
{"some_param": "some_value"}

auth_manager
Added in version 2.7.0.

The auth manager class that airflow should use. Full import path to the auth manager class.

Type:
string

Default:
airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager

Environment Variable:
AIRFLOW__CORE__AUTH_MANAGER

compress_serialized_dags
Added in version 2.3.0.

If True, serialized DAGs are compressed before writing to DB.

Note

This will disable the DAG dependencies view

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__CORE__COMPRESS_SERIALIZED_DAGS

daemon_umask
Added in version 2.3.4.

The default umask to use for process when run in daemon mode (scheduler, worker, etc.)

This controls the file-creation mode mask which determines the initial value of file permission bits for newly created files.

This value is treated as an octal-integer.

Type:
string

Default:
0o077

Environment Variable:
AIRFLOW__CORE__DAEMON_UMASK

dag_discovery_safe_mode
Added in version 1.10.3.

If enabled, Airflow will only scan files containing both DAG and airflow (case-insensitive).

Type:
string

Default:
True

Environment Variable:
AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE

dag_ignore_file_syntax
Added in version 2.3.0.

The pattern syntax used in the .airflowignore files in the DAG directories. Valid values are regexp or glob.

Type:
string

Default:
glob

Environment Variable:
AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX

dag_run_conf_overrides_params
Whether to override params with dag_run.conf. If you pass some key-value pairs through airflow dags backfill -c or airflow dags trigger -c, the key-value pairs will override the existing ones in params.

Type:
string

Default:
True

Environment Variable:
AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS

dagbag_import_error_traceback_depth
Added in version 2.0.0.

If tracebacks are shown, how many entries from the traceback should be shown

Type:
integer

Default:
2

Environment Variable:
AIRFLOW__CORE__DAGBAG_IMPORT_ERROR_TRACEBACK_DEPTH

dagbag_import_error_tracebacks
Added in version 2.0.0.

Should a traceback be shown in the UI for dagbag import errors, instead of just the exception message

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__CORE__DAGBAG_IMPORT_ERROR_TRACEBACKS

dagbag_import_timeout
How long before timing out a python file import

Type:
float

Default:
30.0

Environment Variable:
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT

dags_are_paused_at_creation
Are DAGs paused by default at creation

Type:
string

Default:
True

Environment Variable:
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION

dags_folder
The folder where your airflow pipelines live, most likely a subfolder in a code repository. This path must be absolute.

Type:
string

Default:
{AIRFLOW_HOME}/dags

Environment Variable:
AIRFLOW__CORE__DAGS_FOLDER

default_impersonation
If set, tasks without a run_as_user argument will be run with this user Can be used to de-elevate a sudo user running Airflow when executing tasks

Type:
string

Default:
''

Environment Variable:
AIRFLOW__CORE__DEFAULT_IMPERSONATION

default_pool_task_slot_count
Added in version 2.2.0.

Task Slot counts for default_pool. This setting would not have any effect in an existing deployment where the default_pool is already created. For existing deployments, users can change the number of slots using Webserver, API or the CLI

Type:
integer

Default:
128

Environment Variable:
AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT

default_task_execution_timeout
Added in version 2.3.0.

The default task execution_timeout value for the operators. Expected an integer value to be passed into timedelta as seconds. If not specified, then the value is considered as None, meaning that the operators are never timed out by default.

Type:
integer

Default:
''

Environment Variable:
AIRFLOW__CORE__DEFAULT_TASK_EXECUTION_TIMEOUT

default_task_retries
Added in version 1.10.6.

The number of retries each task is going to have by default. Can be overridden at dag or task level.

Type:
integer

Default:
0

Environment Variable:
AIRFLOW__CORE__DEFAULT_TASK_RETRIES

default_task_retry_delay
Added in version 2.4.0.

The number of seconds each task is going to wait by default between retries. Can be overridden at dag or task level.

Type:
integer

Default:
300

Environment Variable:
AIRFLOW__CORE__DEFAULT_TASK_RETRY_DELAY

default_task_weight_rule
Added in version 2.2.0.

The weighting method used for the effective total priority weight of the task

Type:
string

Default:
downstream

Environment Variable:
AIRFLOW__CORE__DEFAULT_TASK_WEIGHT_RULE

default_timezone
Default timezone in case supplied date times are naive can be UTC (default), system, or any IANA <https://www.iana.org/time-zones> timezone string (e.g. Europe/Amsterdam)

Type:
string

Default:
utc

Environment Variable:
AIRFLOW__CORE__DEFAULT_TIMEZONE

execute_tasks_new_python_interpreter
Added in version 2.0.0.

Should tasks be executed via forking of the parent process

False: Execute via forking of the parent process

True: Spawning a new python process, slower than fork, but means plugin changes picked up by tasks straight away

See also

Available Building Blocks

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER

execution_api_server_url
Added in version 3.0.0.

The url of the execution api server. Default is {BASE_URL}/execution/ where {BASE_URL} is the base url of the API Server. If {BASE_URL} is not set, it will use http://localhost:8080 as the default base url.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__CORE__EXECUTION_API_SERVER_URL

executor
The executor class that airflow should use. Choices include LocalExecutor, CeleryExecutor, KubernetesExecutor or the full import path to the class when using a custom executor.

Type:
string

Default:
LocalExecutor

Environment Variable:
AIRFLOW__CORE__EXECUTOR

fernet_key
Secret key to save connection passwords in the db

Type:
string

Default:
{FERNET_KEY}

Environment Variables:
AIRFLOW__CORE__FERNET_KEY

AIRFLOW__CORE__FERNET_KEY_CMD

AIRFLOW__CORE__FERNET_KEY_SECRET

hide_sensitive_var_conn_fields
Added in version 2.1.0.

Hide sensitive Variables or Connection extra json keys from UI and task logs when set to True

Note

Connection passwords are always hidden in logs

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__CORE__HIDE_SENSITIVE_VAR_CONN_FIELDS

hostname_callable
Hostname by providing a path to a callable, which will resolve the hostname. The format is “package.function”.

For example, default value airflow.utils.net.getfqdn means that result from patched version of socket.getfqdn(), see related CPython Issue.

No argument should be required in the function specified. If using IP address as hostname is preferred, use value airflow.utils.net.get_host_ip_address

Type:
string

Default:
airflow.utils.net.getfqdn

Environment Variable:
AIRFLOW__CORE__HOSTNAME_CALLABLE

killed_task_cleanup_time
When a task is killed forcefully, this is the amount of time in seconds that it has to cleanup after it is sent a SIGTERM, before it is SIGKILLED

Type:
integer

Default:
60

Environment Variable:
AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME

lazy_discover_providers
Added in version 2.0.0.

By default Airflow providers are lazily-discovered (discovery and imports happen only when required). Set it to False, if you want to discover providers whenever ‘airflow’ is invoked via cli or loaded from module.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__CORE__LAZY_DISCOVER_PROVIDERS

lazy_load_plugins
Added in version 2.0.0.

By default Airflow plugins are lazily-loaded (only loaded when required). Set it to False, if you want to load plugins whenever ‘airflow’ is invoked via cli or loaded from module.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__CORE__LAZY_LOAD_PLUGINS

load_examples
Whether to load the DAG examples that ship with Airflow. It’s good to get started, but you probably want to set this to False in a production environment

Type:
string

Default:
True

Environment Variable:
AIRFLOW__CORE__LOAD_EXAMPLES

max_active_runs_per_dag
The maximum number of active DAG runs per DAG. The scheduler will not create more DAG runs if it reaches the limit. This is configurable at the DAG level with max_active_runs, which is defaulted as [core] max_active_runs_per_dag.

Type:
integer

Default:
16

Environment Variable:
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG

max_active_tasks_per_dag
Added in version 2.2.0.

The maximum number of task instances allowed to run concurrently in each dag run. This is also configurable per-dag with max_active_tasks, which is defaulted as [core] max_active_tasks_per_dag.

An example scenario when this would be useful is when you want to stop a new dag run with an early start date from stealing all the executor slots in a cluster.

Type:
integer

Default:
16

Environment Variable:
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG

max_consecutive_failed_dag_runs_per_dag
Added in version 2.9.0.

(experimental) The maximum number of consecutive DAG failures before DAG is automatically paused. This is also configurable per DAG level with max_consecutive_failed_dag_runs, which is defaulted as [core] max_consecutive_failed_dag_runs_per_dag. If not specified, then the value is considered as 0, meaning that the dags are never paused out by default.

Type:
integer

Default:
0

Environment Variable:
AIRFLOW__CORE__MAX_CONSECUTIVE_FAILED_DAG_RUNS_PER_DAG

max_map_length
Added in version 2.3.0.

The maximum list/dict length an XCom can push to trigger task mapping. If the pushed list/dict has a length exceeding this value, the task pushing the XCom will be failed automatically to prevent the mapped tasks from clogging the scheduler.

Type:
integer

Default:
1024

Environment Variable:
AIRFLOW__CORE__MAX_MAP_LENGTH

max_num_rendered_ti_fields_per_task
Added in version 1.10.10.

Maximum number of Rendered Task Instance Fields (Template Fields) per task to store in the Database. All the template_fields for each of Task Instance are stored in the Database. Keeping this number small may cause an error when you try to view Rendered tab in TaskInstance view for older tasks.

Type:
integer

Default:
30

Environment Variable:
AIRFLOW__CORE__MAX_NUM_RENDERED_TI_FIELDS_PER_TASK

max_task_retry_delay
Added in version 2.6.0.

The maximum delay (in seconds) each task is going to wait by default between retries. This is a global setting and cannot be overridden at task or DAG level.

Type:
integer

Default:
86400

Environment Variable:
AIRFLOW__CORE__MAX_TASK_RETRY_DELAY

max_templated_field_length
Added in version 2.9.0.

The maximum length of the rendered template field. If the value to be stored in the rendered template field exceeds this size, it’s redacted.

Type:
integer

Default:
4096

Environment Variable:
AIRFLOW__CORE__MAX_TEMPLATED_FIELD_LENGTH

might_contain_dag_callable
Added in version 2.6.0.

A callable to check if a python file has airflow dags defined or not and should return True if it has dags otherwise False. If this is not provided, Airflow uses its own heuristic rules.

The function should have the following signature

def func_name(file_path: str, zip_file: zipfile.ZipFile | None = None) -> bool: ...
Type:
string

Default:
airflow.utils.file.might_contain_dag_via_default_heuristic

Environment Variable:
AIRFLOW__CORE__MIGHT_CONTAIN_DAG_CALLABLE

min_serialized_dag_fetch_interval
Added in version 1.10.12.

Fetching serialized DAG can not be faster than a minimum interval to reduce database read rate. This config controls when your DAGs are updated in the Webserver

Type:
integer

Default:
10

Environment Variable:
AIRFLOW__CORE__MIN_SERIALIZED_DAG_FETCH_INTERVAL

min_serialized_dag_update_interval
Added in version 1.10.7.

Updating serialized DAG can not be faster than a minimum interval to reduce database write rate.

Type:
integer

Default:
30

Environment Variable:
AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL

mp_start_method
Added in version 2.0.0.

The name of the method used in order to start Python processes via the multiprocessing module. This corresponds directly with the options available in the Python docs: multiprocessing.set_start_method must be one of the values returned by multiprocessing.get_all_start_methods().

Type:
string

Default:
None

Environment Variable:
AIRFLOW__CORE__MP_START_METHOD

Example:
fork

parallelism
This defines the maximum number of task instances that can run concurrently per scheduler in Airflow, regardless of the worker count. Generally this value, multiplied by the number of schedulers in your cluster, is the maximum number of task instances with the running state in the metadata database. The value must be larger or equal 1.

Type:
integer

Default:
32

Environment Variable:
AIRFLOW__CORE__PARALLELISM

plugins_folder
Path to the folder containing Airflow plugins

Type:
string

Default:
{AIRFLOW_HOME}/plugins

Environment Variable:
AIRFLOW__CORE__PLUGINS_FOLDER

security
What security module to use (for example kerberos)

Type:
string

Default:
''

Environment Variable:
AIRFLOW__CORE__SECURITY

sensitive_var_conn_names
Added in version 2.1.0.

A comma-separated list of extra sensitive keywords to look for in variables names or connection’s extra JSON.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES

simple_auth_manager_all_admins
Added in version 3.0.0.

Whether to disable authentication and allow everyone as admin in the environment.

Type:
string

Default:
False

Environment Variable:
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS

simple_auth_manager_passwords_file
Added in version 3.0.0.

The json file where the simple auth manager stores passwords for the configured users. By default this is AIRFLOW_HOME/simple_auth_manager_passwords.json.generated.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE

Example:
/path/to/passwords.json

simple_auth_manager_users
Added in version 3.0.0.

The list of users and their associated role in simple auth manager. If the simple auth manager is used in your environment, this list controls who can access the environment.

List of user-role delimited with a comma. Each user-role is a colon delimited couple of username and role. Roles are predefined in simple auth managers: viewer, user, op, admin.

Type:
string

Default:
admin:admin

Environment Variable:
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS

Example:
bob:admin,peter:viewer

task_success_overtime
Added in version 2.10.0.

Maximum possible time (in seconds) that task will have for execution of auxiliary processes (like listeners, mini scheduler…) after task is marked as success..

Type:
integer

Default:
20

Environment Variable:
AIRFLOW__CORE__TASK_SUCCESS_OVERTIME

test_connection
Added in version 2.7.0.

The ability to allow testing connections across Airflow UI, API and CLI. Supported options: Disabled, Enabled, Hidden. Default: Disabled Disabled - Disables the test connection functionality and disables the Test Connection button in UI. Enabled - Enables the test connection functionality and shows the Test Connection button in UI. Hidden - Disables the test connection functionality and hides the Test Connection button in UI. Before setting this to Enabled, make sure that you review the users who are able to add/edit connections and ensure they are trusted. Connection testing can be done maliciously leading to undesired and insecure outcomes. See Airflow Security Model: Capabilities of authenticated UI users for more details.

Type:
string

Default:
Disabled

Environment Variable:
AIRFLOW__CORE__TEST_CONNECTION

unit_test_mode
Turn unit test mode on (overwrites many configuration options with test values at runtime)

Type:
string

Default:
False

Environment Variable:
AIRFLOW__CORE__UNIT_TEST_MODE

xcom_backend
Added in version 1.10.12.

Path to custom XCom class that will be used to store and resolve operators results

Type:
string

Default:
airflow.sdk.execution_time.xcom.BaseXCom

Environment Variable:
AIRFLOW__CORE__XCOM_BACKEND

Example:
path.to.CustomXCom

dag_file_processor_timeout (Deprecated)
Deprecated since version 3.0: The option has been moved to dag_processor.dag_file_processor_timeout

[dag_processor]
Configuration for the Airflow DAG processor. This includes, for example:
DAG bundles, which allows Airflow to load DAGs from different sources

Parsing configuration, like:
how often to refresh DAGs from those sources

how many files to parse concurrently

bundle_refresh_check_interval
How often the DAG processor should check if any DAG bundles are ready for a refresh, either by hitting the bundles refresh_interval or because another DAG processor has seen a newer version of the bundle. A low value means we check more frequently, and have a smaller window of time where DAG processors are out of sync with each other, parsing different versions of the same bundle.

Type:
integer

Default:
5

Environment Variable:
AIRFLOW__DAG_PROCESSOR__BUNDLE_REFRESH_CHECK_INTERVAL

dag_bundle_config_list
Added in version 3.0.0.

List of backend configs. Must supply name, classpath, and kwargs for each backend.

By default, refresh_interval is set to [dag_processor] refresh_interval, but that can also be overridden in kwargs if desired.

The default is the dags folder dag bundle.

Note: As shown below, you can split your json config over multiple lines by indenting. See configparser documentation for an example: https://docs.python.org/3/library/configparser.html#supported-ini-file-structure.

Type:
string

Default:
[
  {
    "name": "dags-folder",
    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
    "kwargs": {}
  }
]
Environment Variable:
AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST

Example:
[
  {
    "name": "my-git-repo",
    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
    "kwargs": {
      "subdir": "dags",
      "tracking_ref": "main",
      "refresh_interval": 0
    }
  }
]
dag_bundle_storage_path
Added in version 3.0.0.

String path to folder where Airflow bundles can store files locally. Not templated. If no path is provided, Airflow will use Path(tempfile.gettempdir()) / "airflow". This path must be absolute.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH

Example:
/tmp/some-place

dag_file_processor_timeout
How long before timing out a DagFileProcessor, which processes a dag file

Type:
integer

Default:
50

Environment Variable:
AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT

disable_bundle_versioning
Always run tasks with the latest code. If set to True, the bundle version will not be stored on the dag run and therefore, the latest code will always be used.

Note

This setting only applies to bundles that support versioning and does not affect DAG versions displayed in the UI.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__DAG_PROCESSOR__DISABLE_BUNDLE_VERSIONING

file_parsing_sort_mode
One of modified_time, random_seeded_by_host and alphabetical. The DAG processor will list and sort the dag files to decide the parsing order.

modified_time: Sort by modified time of the files. This is useful on large scale to parse the recently modified DAGs first.

random_seeded_by_host: Sort randomly across multiple DAG processors but with same order on the same host, allowing each processor to parse the files in a different order.

alphabetical: Sort by filename

Type:
string

Default:
modified_time

Environment Variable:
AIRFLOW__DAG_PROCESSOR__FILE_PARSING_SORT_MODE

max_callbacks_per_loop
The maximum number of callbacks that are fetched during a single loop.

Type:
integer

Default:
20

Environment Variable:
AIRFLOW__DAG_PROCESSOR__MAX_CALLBACKS_PER_LOOP

min_file_process_interval
Number of seconds after which a DAG file is parsed. The DAG file is parsed every [dag_processor] min_file_process_interval number of seconds. Updates to DAGs are reflected after this interval. Keeping this number low will increase CPU usage.

Type:
integer

Default:
30

Environment Variable:
AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL

parsing_pre_import_modules
Added in version 2.6.0.

The dag_processor reads dag files to extract the airflow modules that are going to be used, and imports them ahead of time to avoid having to re-do it for each parsing process. This flag can be set to False to disable this behavior in case an airflow module needs to be freshly imported each time (at the cost of increased DAG parsing time).

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES

parsing_processes
The DAG processor can run multiple processes in parallel to parse dags. This defines how many processes will run.

Type:
integer

Default:
2

Environment Variable:
AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES

print_stats_interval
How often should DAG processor stats be printed to the logs. Setting to 0 will disable printing stats

Type:
integer

Default:
30

Environment Variable:
AIRFLOW__DAG_PROCESSOR__PRINT_STATS_INTERVAL

refresh_interval
How often (in seconds) to refresh, or look for new files, in a DAG bundle.

Type:
integer

Default:
300

Environment Variable:
AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL

stale_bundle_cleanup_age_threshold
Bundle versions used more recently than this threshold will not be removed. Recency of use is determined by when the task began running on the worker, that age is compared with this setting, given as time delta in seconds.

Type:
integer

Default:
21600

Environment Variable:
AIRFLOW__DAG_PROCESSOR__STALE_BUNDLE_CLEANUP_AGE_THRESHOLD

stale_bundle_cleanup_interval
On shared workers, bundle copies accumulate in local storage as tasks run and version of the bundle changes. This setting represents the delta in seconds between checks for these stale bundles. Bundles which are older than stale_bundle_cleanup_age_threshold may be removed. But we always keep stale_bundle_cleanup_min_versions versions locally. Set to 0 or negative to disable.

Type:
integer

Default:
1800

Environment Variable:
AIRFLOW__DAG_PROCESSOR__STALE_BUNDLE_CLEANUP_INTERVAL

stale_bundle_cleanup_min_versions
Minimum number of local bundle versions to retain on disk. Local bundle versions older than stale_bundle_cleanup_age_threshold will only be deleted we have more than stale_bundle_cleanup_min_versions versions accumulated on the worker.

Type:
integer

Default:
10

Environment Variable:
AIRFLOW__DAG_PROCESSOR__STALE_BUNDLE_CLEANUP_MIN_VERSIONS

stale_dag_threshold
How long (in seconds) to wait after we have re-parsed a DAG file before deactivating stale DAGs (DAGs which are no longer present in the expected files). The reason why we need this threshold is to account for the time between when the file is parsed and when the DAG is loaded. The absolute maximum that this could take is [dag_processor] dag_file_processor_timeout, but when you have a long timeout configured, it results in a significant delay in the deactivation of stale dags.

Type:
integer

Default:
50

Environment Variable:
AIRFLOW__DAG_PROCESSOR__STALE_DAG_THRESHOLD

[database]
alembic_ini_file_path
Added in version 2.7.0.

Path to the alembic.ini file. You can either provide the file path relative to the Airflow home directory or the absolute path if it is located elsewhere.

Type:
string

Default:
alembic.ini

Environment Variable:
AIRFLOW__DATABASE__ALEMBIC_INI_FILE_PATH

check_migrations
Added in version 2.6.0.

Whether to run alembic migrations during Airflow start up. Sometimes this operation can be expensive, and the users can assert the correct version through other means (e.g. through a Helm chart). Accepts True or False.

Type:
string

Default:
True

Environment Variable:
AIRFLOW__DATABASE__CHECK_MIGRATIONS

external_db_managers
Added in version 3.0.0.

List of DB managers to use to migrate external tables in airflow database. The managers must inherit from BaseDBManager. If FabAuthManager is configured in the environment, airflow.providers.fab.auth_manager.models.db.FABDBManager is automatically added.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__DATABASE__EXTERNAL_DB_MANAGERS

Example:
airflow.providers.fab.auth_manager.models.db.FABDBManager

max_db_retries
Added in version 2.3.0.

Number of times the code should be retried in case of DB Operational Errors. Not all transactions will be retried as it can cause undesired state. Currently it is only used in DagFileProcessor.process_file to retry dagbag.sync_to_db.

Type:
integer

Default:
3

Environment Variable:
AIRFLOW__DATABASE__MAX_DB_RETRIES

migration_batch_size
Added in version 3.0.0.

The number of rows to process in each batch when performing a migration. This is useful for large tables to avoid locking and failure due to query timeouts.

Type:
integer

Default:
10000

Environment Variable:
AIRFLOW__DATABASE__MIGRATION_BATCH_SIZE

sql_alchemy_conn
Added in version 2.3.0.

The SQLAlchemy connection string to the metadata database. SQLAlchemy supports many different database engines. See: Set up a Database Backend: Database URI for more details.

Type:
string

Default:
sqlite:///{AIRFLOW_HOME}/airflow.db

Environment Variables:
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_SECRET

sql_alchemy_conn_async
Added in version 3.1.0.

The SQLAlchemy connection string to the metadata database used for async connections. If this is not set, Airflow automatically derives a string by converting sql_alchemy_conn. Unfortunately, this conversion logic does not always work due to various incompatibilities between sync and async db driver implementations. This sets the connection string directly without any conversion instead.

Type:
string

Default:
None

Environment Variables:
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC_CMD

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC_SECRET

Example:
postgresql+asyncpg://postgres:airflow@postgres/airflow

sql_alchemy_connect_args
Added in version 2.3.0.

Import path for connect args in SQLAlchemy. Defaults to an empty dict. This is useful when you want to configure db engine args that SQLAlchemy won’t parse in connection string. This can be set by passing a dictionary containing the create engine parameters. For more details about passing create engine parameters (keepalives variables, timeout etc) in Postgres DB Backend see Setting up a PostgreSQL Database e.g connect_args={"timeout":30} can be defined in airflow_local_settings.py and can be imported as shown below

Changed in 3.1.0: This configuration is only applied to synchronous engines, such as psycopg2. See sql_alchemy_connect_args_async.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS

Example:
airflow_local_settings.connect_args

sql_alchemy_connect_args_async
Added in version 3.1.0.

Import path for connect args in SQLAlchemy. Defaults to an empty dict. This is similar to sql_alchemy_connect_args, but only for async connections.

This configuration is only applied to async engines, such as asyncpg.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS_ASYNC

Example:
airflow_local_settings.connect_args_async

sql_alchemy_engine_args
Added in version 2.3.0.

Extra engine specific keyword args passed to SQLAlchemy’s create_engine, as a JSON-encoded value

Type:
string

Default:
None

Environment Variables:
AIRFLOW__DATABASE__SQL_ALCHEMY_ENGINE_ARGS

AIRFLOW__DATABASE__SQL_ALCHEMY_ENGINE_ARGS_CMD

AIRFLOW__DATABASE__SQL_ALCHEMY_ENGINE_ARGS_SECRET

Example:
{"arg1": true}

sql_alchemy_max_overflow
Added in version 2.3.0.

The maximum overflow size of the pool. When the number of checked-out connections reaches the size set in pool_size, additional connections will be returned up to this limit. When those additional connections are returned to the pool, they are disconnected and discarded. It follows then that the total number of simultaneous connections the pool will allow is pool_size + max_overflow, and the total number of “sleeping” connections the pool will allow is pool_size. max_overflow can be set to -1 to indicate no overflow limit; no limit will be placed on the total number of concurrent connections. Defaults to 10.

Type:
integer

Default:
10

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW

sql_alchemy_pool_enabled
Added in version 2.3.0.

If SQLAlchemy should pool database connections.

Type:
string

Default:
True

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED

sql_alchemy_pool_pre_ping
Added in version 2.3.0.

Check connection at the start of each connection pool checkout. Typically, this is a simple statement like “SELECT 1”. See SQLAlchemy Pooling: Disconnect Handling - Pessimistic for more details.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING

sql_alchemy_pool_recycle
Added in version 2.3.0.

The SQLAlchemy pool recycle is the number of seconds a connection can be idle in the pool before it is invalidated. This config does not apply to sqlite. If the number of DB connections is ever exceeded, a lower config value will allow the system to recover faster.

Type:
integer

Default:
1800

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE

sql_alchemy_pool_size
Added in version 2.3.0.

The SQLAlchemy pool size is the maximum number of database connections in the pool. 0 indicates no limit.

Type:
integer

Default:
5

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE

sql_alchemy_schema
Added in version 2.3.0.

The schema to use for the metadata database. SQLAlchemy supports databases with the concept of multiple schemas.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_SCHEMA

sql_alchemy_session_maker
Added in version 2.10.0.

Important Warning: Use of sql_alchemy_session_maker Highly Discouraged Import path for function which returns ‘sqlalchemy.orm.sessionmaker’. Improper configuration of sql_alchemy_session_maker can lead to serious issues, including data corruption, unrecoverable application crashes. Please review the SQLAlchemy documentation for detailed guidance on proper configuration and best practices.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__DATABASE__SQL_ALCHEMY_SESSION_MAKER

Example:
airflow_local_settings._sessionmaker

sql_engine_collation_for_ids
Added in version 2.3.0.

Collation for dag_id, task_id, key, external_executor_id columns in case they have different encoding. By default this collation is the same as the database collation, however for mysql and mariadb the default is utf8mb3_bin so that the index sizes of our index keys will not exceed the maximum size of allowed index when collation is set to utf8mb4 variant, see GitHub Issue Comment for more details.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__DATABASE__SQL_ENGINE_COLLATION_FOR_IDS

sql_engine_encoding
Added in version 2.3.0.

The encoding for the databases

Type:
string

Default:
utf-8

Environment Variable:
AIRFLOW__DATABASE__SQL_ENGINE_ENCODING

[email]
Configuration email backend and whether to send email alerts on retry or failure

default_email_on_failure
Added in version 2.0.0.

Whether email alerts should be sent when a task failed

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__EMAIL__DEFAULT_EMAIL_ON_FAILURE

default_email_on_retry
Added in version 2.0.0.

Whether email alerts should be sent when a task is retried

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__EMAIL__DEFAULT_EMAIL_ON_RETRY

email_backend
Email backend to use

Type:
string

Default:
airflow.utils.email.send_email_smtp

Environment Variable:
AIRFLOW__EMAIL__EMAIL_BACKEND

email_conn_id
Added in version 2.1.0.

Email connection to use

Type:
string

Default:
smtp_default

Environment Variable:
AIRFLOW__EMAIL__EMAIL_CONN_ID

from_email
Added in version 2.2.4.

Email address that will be used as sender address. It can either be raw email or the complete address in a format Sender Name <sender@email.com>

Type:
string

Default:
None

Environment Variable:
AIRFLOW__EMAIL__FROM_EMAIL

Example:
Airflow <airflow@example.com>

html_content_template
Added in version 2.0.1.

File that will be used as the template for Email content (which will be rendered using Jinja2). If not set, Airflow uses a base template.

See also

Email Configuration

Type:
string

Default:
None

Environment Variable:
AIRFLOW__EMAIL__HTML_CONTENT_TEMPLATE

Example:
/path/to/my_html_content_template_file

ssl_context
Added in version 2.7.0.

ssl context to use when using SMTP and IMAP SSL connections. By default, the context is “default” which sets it to ssl.create_default_context() which provides the right balance between compatibility and security, it however requires that certificates in your operating system are updated and that SMTP/IMAP servers of yours have valid certificates that have corresponding public keys installed on your machines. You can switch it to “none” if you want to disable checking of the certificates, but it is not recommended as it allows MITM (man-in-the-middle) attacks if your infrastructure is not sufficiently secured. It should only be set temporarily while you are fixing your certificate configuration. This can be typically done by upgrading to newer version of the operating system you run Airflow components on,by upgrading/refreshing proper certificates in the OS or by updating certificates for your mail servers.

Type:
string

Default:
default

Environment Variable:
AIRFLOW__EMAIL__SSL_CONTEXT

Example:
default

subject_template
Added in version 2.0.1.

File that will be used as the template for Email subject (which will be rendered using Jinja2). If not set, Airflow uses a base template.

See also

Email Configuration

Type:
string

Default:
None

Environment Variable:
AIRFLOW__EMAIL__SUBJECT_TEMPLATE

Example:
/path/to/my_subject_template_file

[execution_api]
Settings related to the Execution API server.

The ExecutionAPI also uses a lot of settings from the [api_auth] section.

jwt_audience
Added in version 3.0.0.

The audience claim to use when generating and validating JWTs for the Execution API.

This variable can be a single value, or a comma-separated string, in which case the first value is the one that will be used when generating, and the others are accepted at validation time.

Not required, but strongly encouraged

See also jwt_audience

Type:
string

Default:
urn:airflow.apache.org:task

Environment Variable:
AIRFLOW__EXECUTION_API__JWT_AUDIENCE

jwt_expiration_time
Added in version 3.0.0.

Number in seconds until the JWT used for authentication expires. When the token expires, all API calls using this token will fail on authentication.

Make sure that time on ALL the machines that you run airflow components on is synchronized (for example using ntpd) otherwise you might get “forbidden” errors.

Type:
integer

Default:
600

Environment Variable:
AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME

[kerberos]
ccache
Location of your ccache file once kinit has been performed.

Type:
string

Default:
/tmp/airflow_krb5_ccache

Environment Variable:
AIRFLOW__KERBEROS__CCACHE

forwardable
Added in version 2.2.0.

Allow to disable ticket forwardability.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__KERBEROS__FORWARDABLE

include_ip
Added in version 2.2.0.

Allow to remove source IP from token, useful when using token behind NATted Docker host.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__KERBEROS__INCLUDE_IP

keytab
Designates the path to the Kerberos keytab file for the Airflow user

Type:
string

Default:
airflow.keytab

Environment Variable:
AIRFLOW__KERBEROS__KEYTAB

kinit_path
Path to the kinit executable

Type:
string

Default:
kinit

Environment Variable:
AIRFLOW__KERBEROS__KINIT_PATH

principal
gets augmented with fqdn

Type:
string

Default:
airflow

Environment Variable:
AIRFLOW__KERBEROS__PRINCIPAL

reinit_frequency
Determines the frequency at which initialization or re-initialization processes occur.

Type:
string

Default:
3600

Environment Variable:
AIRFLOW__KERBEROS__REINIT_FREQUENCY

[lineage]
backend
what lineage backend to use

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LINEAGE__BACKEND

[logging]
base_log_folder
Added in version 2.0.0.

The folder where airflow should store its log files. This path must be absolute. There are a few existing configurations that assume this is set to the default. If you choose to override this you may need to update the [logging] dag_processor_manager_log_location and [logging] dag_processor_child_process_log_directory settings as well.

Type:
string

Default:
{AIRFLOW_HOME}/logs

Environment Variable:
AIRFLOW__LOGGING__BASE_LOG_FOLDER

callsite_parameters
Added in version 3.1.0.

A comma separated list of information about the callsite (such as line number of filename etc) of logger calls to include in each message.

See structlog.processors.CallsiteParameter for the possible values.The values should be the constant names (FUNC_NAME) or the values (func_name)

Including these in a log message adds a lot to the usability to the logs, but collecting these has a (tiny) cost – if you are super concerned with eking out every last ounce of performance you could turn these off (by setting this value to an empty string)

Type:
string

Default:
filename,lineno

Environment Variable:
AIRFLOW__LOGGING__CALLSITE_PARAMETERS

celery_logging_level
Added in version 2.3.0.

Logging level for celery. If not set, it uses the value of logging_level

Supported values: CRITICAL, ERROR, WARNING, INFO, DEBUG.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__CELERY_LOGGING_LEVEL

celery_stdout_stderr_separation
Added in version 2.7.0.

By default Celery sends all logs into stderr. If enabled any previous logging handlers will get removed. With this option AirFlow will create new handlers and send low level logs like INFO and WARNING to stdout, while sending higher severity logs to stderr.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__LOGGING__CELERY_STDOUT_STDERR_SEPARATION

color_log_error_keywords
Added in version 2.10.0.

A comma separated list of keywords related to errors whose presence should display the line in red color in UI

Type:
string

Default:
error,exception

Environment Variable:
AIRFLOW__LOGGING__COLOR_LOG_ERROR_KEYWORDS

color_log_warning_keywords
Added in version 2.10.0.

A comma separated list of keywords related to warning whose presence should display the line in yellow color in UI

Type:
string

Default:
warn

Environment Variable:
AIRFLOW__LOGGING__COLOR_LOG_WARNING_KEYWORDS

colored_console_log
Added in version 2.0.0.

Flag to enable/disable Colored logs in Console Colour the logs when the controlling terminal is a TTY.

Type:
string

Default:
True

Environment Variable:
AIRFLOW__LOGGING__COLORED_CONSOLE_LOG

dag_processor_child_process_log_directory
Determines the directory where logs for the child processes of the dag processor will be stored

Type:
string

Default:
{AIRFLOW_HOME}/logs/dag_processor

Environment Variable:
AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY

dag_processor_log_format
Added in version 2.4.0.

Format of Dag Processor Log line

Type:
string

Default:
[%%(asctime)s] [SOURCE:DAG_PROCESSOR] {%%(filename)s:%%(lineno)d} %%(levelname)s - %%(message)s

Environment Variable:
AIRFLOW__LOGGING__DAG_PROCESSOR_LOG_FORMAT

dag_processor_log_target
Added in version 2.4.0.

Where to send dag parser logs. If “file”, logs are sent to log files defined by child_process_log_directory.

Type:
string

Default:
file

Environment Variable:
AIRFLOW__LOGGING__DAG_PROCESSOR_LOG_TARGET

delete_local_logs
Added in version 2.6.0.

Whether the local log files for GCS, S3, WASB, HDFS and OSS remote logging should be deleted after they are uploaded to the remote location.

Type:
string

Default:
False

Environment Variable:
AIRFLOW__LOGGING__DELETE_LOCAL_LOGS

encrypt_s3_logs
Added in version 2.0.0.

Use server-side encryption for logs stored in S3

Type:
string

Default:
False

Environment Variable:
AIRFLOW__LOGGING__ENCRYPT_S3_LOGS

extra_logger_names
Added in version 2.0.0.

A comma-separated list of third-party logger names that will be configured to print messages to consoles.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__EXTRA_LOGGER_NAMES

Example:
fastapi,sqlalchemy

fab_logging_level
Added in version 2.0.0.

Logging level for Flask-appbuilder UI.

Supported values: CRITICAL, ERROR, WARNING, INFO, DEBUG.

Type:
string

Default:
WARNING

Environment Variable:
AIRFLOW__LOGGING__FAB_LOGGING_LEVEL

file_task_handler_new_file_permissions
Added in version 2.6.0.

Permissions in the form or of octal string as understood by chmod. The permissions are important when you use impersonation, when logs are written by a different user than airflow. The most secure way of configuring it in this case is to add both users to the same group and make it the default group of both users. Group-writeable logs are default in airflow, but you might decide that you are OK with having the logs other-writeable, in which case you should set it to 0o666. You might decide to add more security if you do not use impersonation and change it to 0o644 to make it only owner-writeable. You can also make it just readable only for owner by changing it to 0o600 if all the access (read/write) for your logs happens from the same user.

Type:
string

Default:
0o664

Environment Variable:
AIRFLOW__LOGGING__FILE_TASK_HANDLER_NEW_FILE_PERMISSIONS

Example:
0o664

file_task_handler_new_folder_permissions
Added in version 2.6.0.

Permissions in the form or of octal string as understood by chmod. The permissions are important when you use impersonation, when logs are written by a different user than airflow. The most secure way of configuring it in this case is to add both users to the same group and make it the default group of both users. Group-writeable logs are default in airflow, but you might decide that you are OK with having the logs other-writeable, in which case you should set it to 0o777. You might decide to add more security if you do not use impersonation and change it to 0o755 to make it only owner-writeable. You can also make it just readable only for owner by changing it to 0o700 if all the access (read/write) for your logs happens from the same user.

Type:
string

Default:
0o775

Environment Variable:
AIRFLOW__LOGGING__FILE_TASK_HANDLER_NEW_FOLDER_PERMISSIONS

Example:
0o775

google_key_path
Added in version 2.0.0.

Path to Google Credential JSON file. If omitted, authorization based on the Application Default Credentials will be used.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__GOOGLE_KEY_PATH

interleave_timestamp_parser
Added in version 2.6.0.

We must parse timestamps to interleave logs between trigger and task. To do so, we need to parse timestamps in log files. In case your log format is non-standard, you may provide import path to callable which takes a string log line and returns the timestamp (datetime.datetime compatible).

Type:
string

Default:
None

Environment Variable:
AIRFLOW__LOGGING__INTERLEAVE_TIMESTAMP_PARSER

Example:
path.to.my_func

log_filename_template
Added in version 2.0.0.

Formatting for how airflow generates file names/paths for each task run.

Type:
string

Default:
dag_id={ ti.dag_id }/run_id={ ti.run_id }/task_id={ ti.task_id }/{%% if ti.map_index >= 0 %%}map_index={ ti.map_index }/{%% endif %%}attempt={ try_number|default(ti.try_number) }.log

Environment Variable:
AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE

log_format
Added in version 2.0.0.

Format of Log line

Changed in 3.1.0: This can now contain color escape sequences %(blue)s etc which will only result in colours if colored_console_log is true.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__LOG_FORMAT

Example:
[%%(asctime)s] {%%(filename)s:%%(lineno)d} %%(levelname)s - %%(message)s

log_formatter_class
Added in version 2.3.4.

Determines the formatter class used by Airflow for structuring its log messages The default formatter class is timezone-aware, which means that timestamps attached to log entries will be adjusted to reflect the local timezone of the Airflow instance

Type:
string

Default:
airflow.utils.log.timezone_aware.TimezoneAware

Environment Variable:
AIRFLOW__LOGGING__LOG_FORMATTER_CLASS

logging_config_class
Added in version 2.0.0.

Logging class Specify the class that will specify the logging configuration This class has to be on the python classpath

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS

Example:
my.path.default_local_settings.LOGGING_CONFIG

logging_level
Added in version 2.0.0.

Logging level.

Supported values: CRITICAL, ERROR, WARNING, INFO, DEBUG.

Type:
string

Default:
INFO

Environment Variable:
AIRFLOW__LOGGING__LOGGING_LEVEL

min_length_masked_secret
Added in version 3.0.0.

The minimum length of a secret to be masked in log messages. Secrets shorter than this length will not be masked.

Type:
integer

Default:
5

Environment Variable:
AIRFLOW__LOGGING__MIN_LENGTH_MASKED_SECRET

remote_base_log_folder
Added in version 2.0.0.

Storage bucket URL for remote logging S3 buckets should start with s3:// Cloudwatch log groups should start with cloudwatch:// GCS buckets should start with gs:// WASB buckets should start with wasb just to help Airflow select correct handler Stackdriver logs should start with stackdriver://

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER

remote_log_conn_id
Added in version 2.0.0.

Users must supply an Airflow connection id that provides access to the storage location. Depending on your remote logging service, this may only be used for reading logs, not writing them.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID

remote_logging
Added in version 2.0.0.

Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search. Set this to True if you want to enable remote logging.

Type:
string

Default:
False

Environment Variable:
AIRFLOW__LOGGING__REMOTE_LOGGING

remote_task_handler_kwargs
Added in version 2.6.0.

The remote_task_handler_kwargs param is loaded into a dictionary and passed to the __init__ of remote task handler and it overrides the values provided by Airflow config. For example if you set delete_local_logs=False and you provide {"delete_local_copy": true}, then the local log files will be deleted after they are uploaded to remote location.

Type:
string

Default:
''

Environment Variables:
AIRFLOW__LOGGING__REMOTE_TASK_HANDLER_KWARGS

AIRFLOW__LOGGING__REMOTE_TASK_HANDLER_KWARGS_CMD

AIRFLOW__LOGGING__REMOTE_TASK_HANDLER_KWARGS_SECRET

Example:
{"delete_local_copy": true}

secret_mask_adapter
Added in version 2.6.0.

An import path to a function to add adaptations of each secret added with airflow.sdk.execution_time.secrets_masker.mask_secret to be masked in log messages. The given function is expected to require a single parameter: the secret to be adapted. It may return a single adaptation of the secret or an iterable of adaptations to each be masked as secrets. The original secret will be masked as well as any adaptations returned.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__SECRET_MASK_ADAPTER

Example:
urllib.parse.quote

simple_log_format
Added in version 2.0.0.

Defines the format of log messages for simple logging configuration

Type:
string

Default:
%%(asctime)s %%(levelname)s - %%(message)s

Environment Variable:
AIRFLOW__LOGGING__SIMPLE_LOG_FORMAT

task_log_prefix_template
Added in version 2.0.0.

Specify prefix pattern like mentioned below with stream handler TaskHandlerWithCustomFormatter

Type:
string

Default:
''

Environment Variable:
AIRFLOW__LOGGING__TASK_LOG_PREFIX_TEMPLATE

Example:
{ti.dag_id}-{ti.task_id}-{logical_date}-{ti.try_number}

task_log_reader
Added in version 2.0.0.

Name of handler to read task instance logs. Defaults to use task handler.

Type:
string

Default:
task

Environment Variable:
AIRFLOW__LOGGING__TASK_LOG_READER

trigger_log_server_port
Added in version 2.6.0.

Port to serve logs from for triggerer. See [logging] worker_log_server_port description for more info.

Type:
string

Default:
8794

Environment Variable:
AIRFLOW__LOGGING__TRIGGER_LOG_SERVER_PORT

worker_log_server_port
Added in version 2.2.0.

When you start an Airflow worker, Airflow starts a tiny web server subprocess to serve the workers local log files to the airflow main web server, who then builds pages and sends them to users. This defines the port on which the logs are served. It needs to be unused, and open visible from the main web server to connect into the workers.

Type:
string

Default:
8793

Environment Variable:
AIRFLOW__LOGGING__WORKER_LOG_SERVER_PORT

[metrics]
StatsD integration settings.

metrics_allow_list
Added in version 2.6.0.

Configure an allow list (comma separated regex patterns to match) to send only certain metrics.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__METRICS__METRICS_ALLOW_LIST

Example:
"scheduler,executor,dagrun,pool,triggerer,celery" or "^scheduler,^executor,heartbeat|timeout"

metrics_block_list
Added in version 2.6.0.

Configure a block list (comma separated regex patterns to match) to block certain metrics from being emitted. If [metrics] metrics_allow_list and [metrics] metrics_block_list are both configured, [metrics] metrics_block_list is ignored.

Type:
string

Default:
''

Environment Variable:
AIRFLOW__METRICS__METRICS_BLOCK_LIST

Example:
"scheduler,executor,dagrun,pool,triggerer,celery" or "^scheduler,^executor,heartbeat|timeout"

otel_debugging_on
Added in version 2.7.0.

If True, all metrics are also emitted to the console. Defaults to False.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__METRICS__OTEL_DEBUGGING_ON

otel_host
Added in version 2.6.0.

Specifies the hostname or IP address of the OpenTelemetry Collector to which Airflow sends metrics and traces.

Type:
string

Default:
localhost

Environment Variable:
AIRFLOW__METRICS__OTEL_HOST

otel_interval_milliseconds
Added in version 2.6.0.

Defines the interval, in milliseconds, at which Airflow sends batches of metrics and traces to the configured OpenTelemetry Collector.

Type:
integer

Default:
60000

Environment Variable:
AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS

otel_on
Added in version 2.6.0.

Enables sending metrics to OpenTelemetry.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__METRICS__OTEL_ON

otel_port
Added in version 2.6.0.

Specifies the port of the OpenTelemetry Collector that is listening to.

Type:
integer

Default:
8889

Environment Variable:
AIRFLOW__METRICS__OTEL_PORT

otel_prefix
Added in version 2.6.0.

The prefix for the Airflow metrics.

Type:
string

Default:
airflow

Environment Variable:
AIRFLOW__METRICS__OTEL_PREFIX

otel_service
Added in version 2.10.3.

The default service name of traces.

Type:
string

Default:
Airflow

Environment Variable:
AIRFLOW__METRICS__OTEL_SERVICE

otel_ssl_active
Added in version 2.7.0.

If True, SSL will be enabled. Defaults to False. To establish an HTTPS connection to the OpenTelemetry collector, you need to configure the SSL certificate and key within the OpenTelemetry collector’s config.yml file.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__METRICS__OTEL_SSL_ACTIVE

stat_name_handler
Added in version 2.0.0.

A function that validate the StatsD stat name, apply changes to the stat name if necessary and return the transformed stat name.

The function should have the following signature

def func_name(stat_name: str) -> str: ...
Type:
string

Default:
''

Environment Variable:
AIRFLOW__METRICS__STAT_NAME_HANDLER

statsd_custom_client_path
Added in version 2.0.0.

If you want to utilise your own custom StatsD client set the relevant module path below. Note: The module path must exist on your PYTHONPATH <https://docs.python.org/3/using/cmdline.html#envvar-PYTHONPATH> for Airflow to pick it up

Type:
string

Default:
None

Environment Variable:
AIRFLOW__METRICS__STATSD_CUSTOM_CLIENT_PATH

statsd_datadog_enabled
Added in version 2.0.0.

To enable datadog integration to send airflow metrics.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__METRICS__STATSD_DATADOG_ENABLED

statsd_datadog_metrics_tags
Added in version 2.6.0.

Set to False to disable metadata tags for some of the emitted metrics

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__METRICS__STATSD_DATADOG_METRICS_TAGS

statsd_datadog_tags
Added in version 2.0.0.

List of datadog tags attached to all metrics(e.g: key1:value1,key2:value2)

Type:
string

Default:
''

Environment Variable:
AIRFLOW__METRICS__STATSD_DATADOG_TAGS

statsd_disabled_tags
Added in version 2.6.0.

If you want to avoid sending all the available metrics tags to StatsD, you can configure a block list of prefixes (comma separated) to filter out metric tags that start with the elements of the list (e.g: job_id,run_id)

Type:
string

Default:
job_id,run_id

Environment Variable:
AIRFLOW__METRICS__STATSD_DISABLED_TAGS

Example:
job_id,run_id,dag_id,task_id

statsd_host
Added in version 2.0.0.

Specifies the host address where the StatsD daemon (or server) is running

Type:
string

Default:
localhost

Environment Variable:
AIRFLOW__METRICS__STATSD_HOST

statsd_influxdb_enabled
Added in version 2.6.0.

To enable sending Airflow metrics with StatsD-Influxdb tagging convention.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__METRICS__STATSD_INFLUXDB_ENABLED

statsd_ipv6
Added in version 3.0.0.

Enables the statsd host to be resolved into IPv6 address

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__METRICS__STATSD_IPV6

statsd_on
Added in version 2.0.0.

Enables sending metrics to StatsD.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__METRICS__STATSD_ON

statsd_port
Added in version 2.0.0.

Specifies the port on which the StatsD daemon (or server) is listening to

Type:
integer

Default:
8125

Environment Variable:
AIRFLOW__METRICS__STATSD_PORT

statsd_prefix
Added in version 2.0.0.

Defines the namespace for all metrics sent from Airflow to StatsD

Type:
string

Default:
airflow

Environment Variable:
AIRFLOW__METRICS__STATSD_PREFIX

[operators]
default_cpus
Indicates the default number of CPU units allocated to each operator when no specific CPU request is specified in the operator’s configuration

Type:
string

Default:
1

Environment Variable:
AIRFLOW__OPERATORS__DEFAULT_CPUS

default_deferrable
Added in version 2.7.0.

The default value of attribute “deferrable” in operators and sensors.

Type:
boolean

Default:
false

Environment Variable:
AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE

default_disk
Indicates the default number of disk storage allocated to each operator when no specific disk request is specified in the operator’s configuration

Type:
string

Default:
512

Environment Variable:
AIRFLOW__OPERATORS__DEFAULT_DISK

default_gpus
Indicates the default number of GPUs allocated to each operator when no specific GPUs request is specified in the operator’s configuration

Type:
string

Default:
0

Environment Variable:
AIRFLOW__OPERATORS__DEFAULT_GPUS

default_owner
The default owner assigned to each new operator, unless provided explicitly or passed via default_args

Type:
string

Default:
airflow

Environment Variable:
AIRFLOW__OPERATORS__DEFAULT_OWNER

default_queue
Added in version 2.1.0.

Default queue that tasks get assigned to and that worker listen on.

Type:
string

Default:
default

Environment Variable:
AIRFLOW__OPERATORS__DEFAULT_QUEUE

default_ram
Indicates the default number of RAM allocated to each operator when no specific RAM request is specified in the operator’s configuration

Type:
string

Default:
512

Environment Variable:
AIRFLOW__OPERATORS__DEFAULT_RAM

[scheduler]
allowed_run_id_pattern
Added in version 2.6.3.

The run_id pattern used to verify the validity of user input to the run_id parameter when triggering a DAG. This pattern cannot change the pattern used by scheduler to generate run_id for scheduled DAG runs or DAG runs triggered without changing the run_id parameter.

Type:
string

Default:
^[A-Za-z0-9_.~:+-]+$

Environment Variable:
AIRFLOW__SCHEDULER__ALLOWED_RUN_ID_PATTERN

catchup_by_default
Turn on scheduler catchup by setting this to True. Default behavior is unchanged and Command Line Backfills still work, but the scheduler will not do scheduler catchup if this is False, however it can be set on a per DAG basis in the DAG definition (catchup)

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT

create_cron_data_intervals
Added in version 2.9.0.

Whether to create DAG runs that span an interval or one single point in time for cron schedules, when a cron string is provided to schedule argument of a DAG.

True: CronDataIntervalTimetable is used, which is suitable for DAGs with well-defined data interval. You get contiguous intervals from the end of the previous interval up to the scheduled datetime.

False: CronTriggerTimetable is used, which is closer to the behavior of cron itself.

Notably, for CronTriggerTimetable, the logical date is the same as the time the DAG Run will try to schedule, while for CronDataIntervalTimetable, the logical date is the beginning of the data interval, but the DAG Run will try to schedule at the end of the data interval.

See also

Differences between “trigger” and “data interval” timetables

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS

create_delta_data_intervals
Added in version 2.11.0.

Whether to create DAG runs that span an interval or one single point in time when a timedelta or relativedelta is provided to schedule argument of a DAG.

True: DeltaDataIntervalTimetable is used, which is suitable for DAGs with well-defined data interval. You get contiguous intervals from the end of the previous interval up to the scheduled datetime.

False: DeltaTriggerTimetable is used, which is suitable for DAGs that simply want to say e.g. “run this every day” and do not care about the data interval.

Notably, for DeltaTriggerTimetable, the logical date is the same as the time the DAG Run will try to schedule, while for DeltaDataIntervalTimetable, the logical date is the beginning of the data interval, but the DAG Run will try to schedule at the end of the data interval.

See also

Differences between “trigger” and “data interval” timetables

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__SCHEDULER__CREATE_DELTA_DATA_INTERVALS

enable_health_check
Added in version 2.4.0.

When you start a scheduler, airflow starts a tiny web server subprocess to serve a health check if this is set to True

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK

enable_tracemalloc
Added in version 3.0.0.

Whether to enable memory allocation tracing in the scheduler. If enabled, Airflow will start tracing memory allocation and log the top 10 memory usages at the error level upon receiving the signal SIGUSR1. This is an expensive operation and generally should not be used except for debugging purposes.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__SCHEDULER__ENABLE_TRACEMALLOC

ignore_first_depends_on_past_by_default
Added in version 2.3.0.

Setting this to True will make first task instance of a task ignore depends_on_past setting. A task instance will be considered as the first task instance of a task when there is no task instance in the DB with a logical_date earlier than it., i.e. no manual marking success will be needed for a newly added task to be scheduled.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__SCHEDULER__IGNORE_FIRST_DEPENDS_ON_PAST_BY_DEFAULT

job_heartbeat_sec
Task instances listen for external kill signal (when you clear tasks from the CLI or the UI), this defines the frequency at which they should listen (in seconds).

Type:
float

Default:
5

Environment Variable:
AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC

max_dagruns_per_loop_to_schedule
Added in version 2.0.0.

How many DagRuns should a scheduler examine (and lock) when scheduling and queuing tasks.

See also

Scheduler Configuration options

Type:
integer

Default:
20

Environment Variable:
AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE

max_dagruns_to_create_per_loop
Added in version 2.0.0.

Max number of DAGs to create DagRuns for per scheduler loop.

See also

Scheduler Configuration options

Type:
integer

Default:
10

Environment Variable:
AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP

max_tis_per_query
This determines the number of task instances to be evaluated for scheduling during each scheduler loop. Set this to 0 to use the value of [core] parallelism

Type:
integer

Default:
16

Environment Variable:
AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY

num_runs
Added in version 1.10.6.

The number of times to try to schedule each DAG file -1 indicates unlimited number

Type:
integer

Default:
-1

Environment Variable:
AIRFLOW__SCHEDULER__NUM_RUNS

orphaned_tasks_check_interval
Added in version 2.0.0.

How often (in seconds) should the scheduler check for orphaned tasks and SchedulerJobs

Type:
float

Default:
300.0

Environment Variable:
AIRFLOW__SCHEDULER__ORPHANED_TASKS_CHECK_INTERVAL

parsing_cleanup_interval
Added in version 2.5.0.

How often (in seconds) to check for stale DAGs (DAGs which are no longer present in the expected files) which should be deactivated, as well as assets that are no longer referenced and should be marked as orphaned.

Type:
integer

Default:
60

Environment Variable:
AIRFLOW__SCHEDULER__PARSING_CLEANUP_INTERVAL

pool_metrics_interval
Added in version 2.0.0.

How often (in seconds) should pool usage stats be sent to StatsD (if statsd_on is enabled)

Type:
float

Default:
5.0

Environment Variable:
AIRFLOW__SCHEDULER__POOL_METRICS_INTERVAL

running_metrics_interval
Added in version 3.0.0.

How often (in seconds) should running task instance stats be sent to StatsD (if statsd_on is enabled)

Type:
float

Default:
30.0

Environment Variable:
AIRFLOW__SCHEDULER__RUNNING_METRICS_INTERVAL

scheduler_health_check_server_host
Added in version 2.8.0.

When you start a scheduler, airflow starts a tiny web server subprocess to serve a health check on this host

Type:
string

Default:
0.0.0.0

Environment Variable:
AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_SERVER_HOST

scheduler_health_check_server_port
Added in version 2.4.0.

When you start a scheduler, airflow starts a tiny web server subprocess to serve a health check on this port

Type:
integer

Default:
8974

Environment Variable:
AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_SERVER_PORT

scheduler_health_check_threshold
Added in version 1.10.2.

If the last scheduler heartbeat happened more than [scheduler] scheduler_health_check_threshold ago (in seconds), scheduler is considered unhealthy. This is used by the health check in the /health endpoint and in airflow jobs check CLI for SchedulerJob.

Type:
integer

Default:
30

Environment Variable:
AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD

scheduler_heartbeat_sec
The scheduler constantly tries to trigger new tasks (look at the scheduler section in the docs for more information). This defines how often the scheduler should run (in seconds).

Type:
integer

Default:
5

Environment Variable:
AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC

scheduler_idle_sleep_time
Added in version 2.2.0.

Controls how long the scheduler will sleep between loops, but if there was nothing to do in the loop. i.e. if it scheduled something then it will start the next loop iteration straight away.

Type:
float

Default:
1

Environment Variable:
AIRFLOW__SCHEDULER__SCHEDULER_IDLE_SLEEP_TIME

task_instance_heartbeat_sec
Added in version 2.7.0.

The frequency (in seconds) at which the LocalTaskJob should send heartbeat signals to the scheduler to notify it’s still alive. If this value is set to 0, the heartbeat interval will default to the value of [scheduler] task_instance_heartbeat_timeout.

Type:
integer

Default:
0

Environment Variable:
AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_SEC

task_instance_heartbeat_timeout
Local task jobs periodically heartbeat to the DB. If the job has not heartbeat in this many seconds, the scheduler will mark the associated task instance as failed and will re-schedule the task.

Type:
integer

Default:
300

Environment Variable:
AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT

task_instance_heartbeat_timeout_detection_interval
Added in version 2.3.0.

How often (in seconds) should the scheduler check for task instances whose heartbeats have timed out.

Type:
float

Default:
10.0

Environment Variable:
AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL

task_queued_timeout
Added in version 2.6.0.

Amount of time a task can be in the queued state before being retried or set to failed.

Type:
float

Default:
600.0

Environment Variable:
AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT

task_queued_timeout_check_interval
Added in version 2.6.0.

How often to check for tasks that have been in the queued state for longer than [scheduler] task_queued_timeout.

Type:
float

Default:
120.0

Environment Variable:
AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT_CHECK_INTERVAL

trigger_timeout_check_interval
Added in version 2.2.0.

How often to check for expired trigger requests that have not run yet.

Type:
float

Default:
15

Environment Variable:
AIRFLOW__SCHEDULER__TRIGGER_TIMEOUT_CHECK_INTERVAL

use_job_schedule
Added in version 1.10.2.

Turn off scheduler use of cron intervals by setting this to False. DAGs submitted manually in the web UI or with trigger_dag will still run.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE

use_row_level_locking
Added in version 2.0.0.

Should the scheduler issue SELECT ... FOR UPDATE in relevant queries. If this is set to False then you should not run more than a single scheduler at once

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__SCHEDULER__USE_ROW_LEVEL_LOCKING

dag_dir_list_interval (Deprecated)
Deprecated since version 3.0: The option has been moved to dag_processor.refresh_interval

parsing_pre_import_modules (Deprecated)
Deprecated since version 3.0.4: The option has been moved to dag_processor.parsing_pre_import_modules

[secrets]
backend
Added in version 1.10.10.

Full class name of secrets backend to enable (will precede env vars and metastore in search path)

Type:
string

Default:
''

Environment Variable:
AIRFLOW__SECRETS__BACKEND

Example:
airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend

backend_kwargs
Added in version 1.10.10.

The backend_kwargs param is loaded into a dictionary and passed to __init__ of secrets backend class. See documentation for the secrets backend you are using. JSON is expected.

Example for AWS Systems Manager ParameterStore: {"connections_prefix": "/airflow/connections", "profile_name": "default"}

Type:
string

Default:
''

Environment Variables:
AIRFLOW__SECRETS__BACKEND_KWARGS

AIRFLOW__SECRETS__BACKEND_KWARGS_CMD

AIRFLOW__SECRETS__BACKEND_KWARGS_SECRET

cache_ttl_seconds
Added in version 2.7.0.

Note

This is an experimental feature.

When the cache is enabled, this is the duration for which we consider an entry in the cache to be valid. Entries are refreshed if they are older than this many seconds. It means that when the cache is enabled, this is the maximum amount of time you need to wait to see a Variable change take effect.

Type:
integer

Default:
900

Environment Variable:
AIRFLOW__SECRETS__CACHE_TTL_SECONDS

use_cache
Added in version 2.7.0.

Note

This is an experimental feature.

Enables local caching of Variables, when parsing DAGs only. Using this option can make dag parsing faster if Variables are used in top level code, at the expense of longer propagation time for changes. Please note that this cache concerns only the DAG parsing step. There is no caching in place when DAG tasks are run.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__SECRETS__USE_CACHE

[sensors]
default_timeout
Added in version 2.3.0.

Sensor default timeout, 7 days by default (7 * 24 * 60 * 60).

Type:
float

Default:
604800

Environment Variable:
AIRFLOW__SENSORS__DEFAULT_TIMEOUT

[sentry]
Sentry integration. Here you can supply additional configuration options based on the Python platform. See Python / Configuration / Basic Options for more details. Unsupported options: integrations, in_app_include, in_app_exclude, ignore_errors, before_breadcrumb, transport.

before_send
Added in version 2.2.0.

Dotted path to a before_send function that the sentry SDK should be configured to use.

Type:
string

Default:
None

Environment Variable:
AIRFLOW__SENTRY__BEFORE_SEND

sentry_dsn
Added in version 1.10.6.

Type:
string

Default:
''

Environment Variables:
AIRFLOW__SENTRY__SENTRY_DSN

AIRFLOW__SENTRY__SENTRY_DSN_CMD

AIRFLOW__SENTRY__SENTRY_DSN_SECRET

sentry_on
Added in version 2.0.0.

Enable error reporting to Sentry

Type:
boolean

Default:
false

Environment Variable:
AIRFLOW__SENTRY__SENTRY_ON

[smtp]
If you want airflow to send emails on retries, failure, and you want to use the airflow.utils.email.send_email_smtp function, you have to configure an smtp server here

smtp_host
Specifies the host server address used by Airflow when sending out email notifications via SMTP.

Type:
string

Default:
localhost

Environment Variable:
AIRFLOW__SMTP__SMTP_HOST

smtp_mail_from
Specifies the default from email address used when Airflow sends email notifications.

Type:
string

Default:
airflow@example.com

Environment Variable:
AIRFLOW__SMTP__SMTP_MAIL_FROM

smtp_port
Defines the port number on which Airflow connects to the SMTP server to send email notifications.

Type:
integer

Default:
25

Environment Variable:
AIRFLOW__SMTP__SMTP_PORT

smtp_retry_limit
Added in version 2.0.0.

Defines the maximum number of times Airflow will attempt to connect to the SMTP server.

Type:
integer

Default:
5

Environment Variable:
AIRFLOW__SMTP__SMTP_RETRY_LIMIT

smtp_ssl
Determines whether to use an SSL connection when talking to the SMTP server.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__SMTP__SMTP_SSL

smtp_starttls
Determines whether to use the STARTTLS command when connecting to the SMTP server.

Type:
boolean

Default:
True

Environment Variable:
AIRFLOW__SMTP__SMTP_STARTTLS

smtp_timeout
Added in version 2.0.0.

Determines the maximum time (in seconds) the Apache Airflow system will wait for a connection to the SMTP server to be established.

Type:
integer

Default:
30

Environment Variable:
AIRFLOW__SMTP__SMTP_TIMEOUT

[traces]
Distributed traces integration settings.

otel_debug_traces_on
Added in version 3.1.0.

If True, then traces from Airflow internal methods are exported. Defaults to False.

Type:
string

Default:
False

Environment Variable:
AIRFLOW__TRACES__OTEL_DEBUG_TRACES_ON

otel_debugging_on
Added in version 2.10.0.

If True, all traces are also emitted to the console. Defaults to False.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__TRACES__OTEL_DEBUGGING_ON

otel_host
Added in version 2.10.0.

Specifies the hostname or IP address of the OpenTelemetry Collector to which Airflow sends traces.

Type:
string

Default:
localhost

Environment Variable:
AIRFLOW__TRACES__OTEL_HOST

otel_on
Added in version 2.10.0.

Enables sending traces to OpenTelemetry.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__TRACES__OTEL_ON

otel_port
Added in version 2.10.0.

Specifies the port of the OpenTelemetry Collector that is listening to.

Type:
integer

Default:
8889

Environment Variable:
AIRFLOW__TRACES__OTEL_PORT

otel_service
Added in version 2.10.0.

The default service name of traces.

Type:
string

Default:
Airflow

Environment Variable:
AIRFLOW__TRACES__OTEL_SERVICE

otel_ssl_active
Added in version 2.10.0.

If True, SSL will be enabled. Defaults to False. To establish an HTTPS connection to the OpenTelemetry collector, you need to configure the SSL certificate and key within the OpenTelemetry collector’s config.yml file.

Type:
boolean

Default:
False

Environment Variable:
AIRFLOW__TRACES__OTEL_SSL_ACTIVE

[triggerer]
capacity
Added in version 2.2.0.

How many triggers a single Triggerer will run at once, by default.

Type:
integer

Default:
1000

Environment Variable:
AIRFLOW__TRIGGERER__CAPACITY

job_heartbeat_sec
Added in version 2.6.3.

How often to heartbeat the Triggerer job to ensure it hasn’t been killed.

Type:
float

Default:
5

Environment Variable:
AIRFLOW__TRIGGERER__JOB_HEARTBEAT_SEC

triggerer_health_check_threshold
Added in version 2.7.0.

If the last triggerer heartbeat happened more than [triggerer] triggerer_health_check_threshold ago (in seconds), triggerer is considered unhealthy. This is used by the health check in the /health endpoint and in airflow jobs check CLI for TriggererJob.

Type:
float

Default:
30

Environment Variable:
AIRFLOW__TRIGGERER__TRIGGERER_HEALTH_CHECK_THRESHOLD

default_capacity (Deprecated)
Deprecated since version 3.0: The option has been moved to triggerer.capacity

[workers]
Configuration related to workers that run Airflow tasks.

execution_api_retries
Added in version 3.0.0.

The maximum number of retry attempts to the execution API server.

Type:
integer

Default:
5

Environment Variable:
AIRFLOW__WORKERS__EXECUTION_API_RETRIES

execution_api_retry_wait_max
Added in version 3.0.0.

The maximum amount of time (in seconds) to wait before retrying a failed API request.

Type:
float

Default:
90.0

Environment Variable:
AIRFLOW__WORKERS__EXECUTION_API_RETRY_WAIT_MAX

execution_api_retry_wait_min
Added in version 3.0.0.

The minimum amount of time (in seconds) to wait before retrying a failed API request.

Type:
float

Default:
1.0

Environment Variable:
AIRFLOW__WORKERS__EXECUTION_API_RETRY_WAIT_MIN

execution_api_timeout
Added in version 3.1.1.

The timeout (in seconds) for HTTP requests from workers to the Execution API server. This controls how long a worker will wait for a response from the API server before timing out. Increase this value if you experience timeout errors under high load.

Type:
float

Default:
5.0

Environment Variable:
AIRFLOW__WORKERS__EXECUTION_API_TIMEOUT

max_failed_heartbeats
Added in version 3.0.0.

The maximum number of consecutive failed heartbeats before terminating the task instance process.

Type:
integer

Default:
3

Environment Variable:
AIRFLOW__WORKERS__MAX_FAILED_HEARTBEATS

min_heartbeat_interval
Added in version 3.0.0.

The minimum interval (in seconds) at which the worker checks the task instance’s heartbeat status with the API server to confirm it is still alive.

Type:
integer

Default:
5

Environment Variable:
AIRFLOW__WORKERS__MIN_HEARTBEAT_INTERVAL

secrets_backend
Added in version 3.0.0.

Full class name of secrets backend to enable for workers (will precede env vars backend)

Type:
string

Default:
''

Environment Variable:
AIRFLOW__WORKERS__SECRETS_BACKEND

Example:
airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend

secrets_backend_kwargs
Added in version 3.0.0.

The secrets_backend_kwargs param is loaded into a dictionary and passed to __init__ of secrets backend class. See documentation for the secrets backend you are using. JSON is expected.

Example for AWS Systems Manager ParameterStore: {"connections_prefix": "/airflow/connections", "profile_name": "default"}

Type:
string

Default:
''

Environment Variables:
AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS

AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS_CMD

AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS_SECRET

socket_cleanup_timeout
Added in version 3.0.3.

Number of seconds to wait after a task process exits before forcibly closing any remaining communication sockets. This helps prevent the task supervisor from hanging indefinitely due to missed EOF signals.

Type:
float

Default:
60.0

Environment Variable:
AIRFLOW__WORKERS__SOCKET_CLEANUP_TIMEOUT
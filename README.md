# watchywatchy condense

part of the 'watchywatchy' distributed filesystem monitoring proof of concept

[link to documentation (German)](https://github.com/pstmps/watchywatchy/blob/main/20240517_watchywatchy.pdf)

## Description

Condenses a stream of filesystem events generated by a fleet of elasticagents into a representation of the filesystem

Since the elasticagent writes all filesystem changes as events to the index, as a file changes, only records are added.
When file sizes are summed up, the filesizes are incorrect.
This *could* be fixed by a complicated query that aggregates the entries and only selects the newest timestamp records, but as the index gets larger, the query time could increase exponentially.

Also, when directories are deleted, only a single record is added to the index, not a record for every file contained in the directory.

To solve these problems, the condensing operation first executes a paginated aggregation query and looks for every filesystem entry with more than one record.
These are then queried seperately and either

condensed - every entry is deleted except the last one
or
deleted - every entry is deleted

The 'health' of the index can be queried by aggregating and checking how many files or directories have more than one record.
Ideally there should be none.

For the condenser to work properly the elasticagents must have a pipeline configured in a certain way detailed in the documentation above, for all fields to be available.

Source code for the `painless` pipeline:

[pipeline](https://github.com/pstmps/watchywatchy/tree/main/elastic_conf)

## Technical details

The program uses a tokio runtime and passes events defined via a message enum defined in `src/message.rs`.

In a central loop, the messages are received and handled accordingly.

The `process_events` function matches event types via a `match event` clause.

My main aim was to understand programming async rust programs with tokio and mpsc channels.


## Usage

It is recommended to run the program as a service.

The parameters are configured via a .env file:

```
# Condense configuration
CONDENSE_LOG_PATH=/opt/watchy_condense/log
CONDENSE_LOG_TO_CONSOLE=True
RUST_LOG=info
CONDENSE_INDEX=.ds-logs-fim.event-default*
# channel size
CONDENSE_ACTION_BUFFER=1024
# how many delete events to buffer before sending to ES
CONDENSE_DELETE_BUFFER=100
# how many events to fetch from ES at a time
CONDENSE_PAGE_SIZE=256
# how long to wait (in seconds) before sending delete events to ES if the buffer is not full
CONDENSE_DELETE_TIMEOUT=5
# how long (in seconds) to sleep between aggregation runs
CONDENSE_AGGREGATION_SLEEP=360

# Elasticsearch configuration
#CERT_PATH=/etc/ssl/certs/http_ca.crt
CERT_PATH=/opt/watchy_condense/http_ca.crt
ES_IP=192.168.2.193
ES_USER=elastic
ES_PASSWORD=meinpasswort123
```

And a service file like the example below:

```
[Unit]
Description=A Condenser for filesystem events
After=network.target

[Service]
EnvironmentFile=/opt/watchy_condense/.env
ExecStart=/opt/watchy_condense/watchy_condense_rs
Restart=always
RestartSec=10
LimitNOFILE=4096

[Install]
WantedBy=multi-user.target
```
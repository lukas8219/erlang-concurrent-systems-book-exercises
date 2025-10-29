-module(dqueue_msg_store).

-behaviour(gen_server).

-record(dqueue_msg_store, {ts_identifier, content}).

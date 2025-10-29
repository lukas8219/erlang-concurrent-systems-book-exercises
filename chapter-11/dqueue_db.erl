-module(dqueue_db).

-export([handle_call/3, handle_cast/2, init/1, start_database/1]).

-behavior(gen_server).

-record(dqueue_db, {table_name}).
-record(db_state, {identifier, in_flight = [], idle_consumers = []}).

start_database(QueueName) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, QueueName, []).

%% how to handle name
init(QueueName) ->
    TableName = create_table_if_not_exists(QueueName),
    {ok, #dqueue_db{table_name = TableName}}.

create_table_if_not_exists(Name) ->
    case mnesia:create_table(Name,
                             [{ram_only_copies, nodes()},
                              {attributes, [record_info(fields, db_state)]}])
    of
        {atomic, ok} ->
            io:format("Table ~p created successfully.~n", [Name]),
            Name;
        {aborted, {already_exists, _}} ->
            io:format("Table ~p already exists.~n", [Name]);
        {aborted, Reason} ->
            io:format("Error creating table ~p: ~p~n", [Name, Reason])
    end.

handle_call({in_flight, add, {_Ts, _Content}},
            _Ref,
            _State = #dqueue_db{table_name = _TableName}) ->
    ok.

handle_cast(_, _) ->
    ok.

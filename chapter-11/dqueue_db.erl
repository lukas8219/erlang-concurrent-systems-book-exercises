-module(dqueue_db).

-export([add_idle_consumer/1, remove_idle_consumer/1, add_inflight_message/1,
         remove_inflight_message/1]).
-export([handle_call/3, handle_cast/2, init/1, start_database/1]).

-behavior(gen_server).

-record(dqueue_db_state, {table_name, db_store_state}).
-record(db_state, {identifier, in_flight = [], idle_consumers = []}).

start_database(QueueName) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, QueueName, []).

%% how to handle name
init(QueueName) ->
    {ok, TableName} = create_table_if_not_exists(QueueName),
    {ok,
     #dqueue_db_state{table_name = TableName,
                      db_store_state =
                          #db_state{identifier = QueueName,
                                    in_flight = [],
                                    idle_consumers = []}}}.

create_table_if_not_exists(Name) ->
    case mnesia:create_table(Name,
                             [{ram_only_copies, nodes()},
                              {attributes, [record_info(fields, db_state)]}])
    of
        {atomic, ok} ->
            io:format("Table ~p created successfully.~n", [Name]),
            {ok, Name};
        {aborted, {already_exists, _}} ->
            io:format("Table ~p already exists.~n", [Name]);
        {aborted, Reason} ->
            io:format("Error creating table ~p: ~p~n", [Name, Reason])
    end.

handle_call({in_flight, add, {Ts, Content}},
            _Ref,
            State =
                #dqueue_db_state{table_name = TableName,
                                 db_store_state = #db_state{in_flight = InFlight}}) ->
    NewInflight = [{Ts, Content} | InFlight],
    NewState = State#dqueue_db_state{db_store_state = #db_state{in_flight = NewInflight}},
    mnesia:write(TableName, db_store_state),
    {reply, ok, NewState};
handle_call({in_flight, remove, {Ts, _}},
            _Ref,
            State =
                #dqueue_db_state{table_name = TableName,
                                 db_store_state = #db_state{in_flight = InFlight}}) ->
    NewInFlight =
        [{InflightMTs, InflightContent}
         || {InflightMTs, InflightContent} <- InFlight, InflightMTs =/= Ts],
    NewState = State#dqueue_db_state{db_store_state = #db_state{in_flight = NewInFlight}},
    mnesia:write(TableName, db_store_state),
    {reply, ok, NewState};
handle_call({idle_consumers, add, {CPid}},
            _Ref,
            State =
                #dqueue_db_state{table_name = TableName,
                                 db_store_state = #db_state{idle_consumers = IdleConsumers}}) ->
    NewConsumers = [CPid | IdleConsumers],
    NewState =
        State#dqueue_db_state{db_store_state = #db_state{idle_consumers = NewConsumers}},
    mnesia:write(TableName, db_store_state),
    {reply, ok, NewState};
handle_call({idle_consumers, remove, {DCPid}},
            _Ref,
            State =
                #dqueue_db_state{table_name = TableName,
                                 db_store_state = #db_state{idle_consumers = IdleConsumers}}) ->
    NewIdleConsumers = [CPid || CPid <- IdleConsumers, CPid =/= DCPid],
    NewState =
        State#dqueue_db_state{db_store_state = #db_state{idle_consumers = NewIdleConsumers}},
    mnesia:write(TableName, db_store_state),
    {reply, ok, NewState}.

handle_cast(_, _) ->
    ok.

%% Client APIs
%%

add_idle_consumer(Pid) ->
    gen_server:call({idle_consumers, add}, Pid).

remove_idle_consumer(Pid) ->
    gen_server:call({idle_consumers, remove}, Pid).

add_inflight_message(Message) ->
    gen_server:call({in_flight, add}, Message).

remove_inflight_message(Message) ->
    gen_server:call({in_flight, remove}, Message).

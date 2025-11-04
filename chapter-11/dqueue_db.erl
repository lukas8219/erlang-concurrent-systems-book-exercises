-module(dqueue_db).

-export([add_idle_consumer/1, remove_idle_consumer/1, add_inflight_message/1,
         remove_inflight_message/1, send_if_any_idle/1]).
-export([handle_call/3, handle_cast/2, init/1, start_database/1]).

-behavior(gen_server).

-record(dqueue_db_state, {table_name, db_store_state}).
-record(db_state, {identifier, in_flight = [], idle_consumers = []}).

start_database(QueueName) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, QueueName, []).

%% how to handle name
%% this state is not distributed yet.
init(QueueName) ->
    {ok,
     #dqueue_db_state{table_name = "mocked",
                      db_store_state =
                          #db_state{identifier = QueueName,
                                    in_flight = [],
                                    idle_consumers = []}}}.

handle_call({in_flight, add, {Ts, _H, Content}},
            _Ref,
            State = #dqueue_db_state{db_store_state = #db_state{in_flight = InFlight}}) ->
    NewInflight = [{Ts, Content} | InFlight],
    NewState = State#dqueue_db_state{db_store_state = #db_state{in_flight = NewInflight}},
    {reply, ok, NewState};
handle_call({in_flight, remove, {Ts, _H, _C}},
            _Ref,
            State = #dqueue_db_state{db_store_state = #db_state{in_flight = InFlight}}) ->
    NewInFlight =
        [{InflightMTs, InflightContent}
         || {InflightMTs, InflightContent} <- InFlight, InflightMTs =/= Ts],
    NewState = State#dqueue_db_state{db_store_state = #db_state{in_flight = NewInFlight}},
    {reply, ok, NewState};
handle_call({idle_consumers, add, CPid},
            _Ref,
            State = #dqueue_db_state{db_store_state = #db_state{idle_consumers = IdleConsumers}}) ->
    NewConsumers = [CPid | IdleConsumers],
    NewState =
        State#dqueue_db_state{db_store_state = #db_state{idle_consumers = NewConsumers}},
    {reply, ok, NewState};
handle_call({idle_consumers, remove, DCPid},
            _Ref,
            State = #dqueue_db_state{db_store_state = #db_state{idle_consumers = IdleConsumers}}) ->
    NewIdleConsumers = [CPid || CPid <- IdleConsumers, CPid =/= DCPid],
    NewState =
        State#dqueue_db_state{db_store_state = #db_state{idle_consumers = NewIdleConsumers}},
    {reply, ok, NewState};
handle_call({send_idle, Message},
            _Ref,
            State =
                #dqueue_db_state{db_store_state =
                                     #db_state{idle_consumers = [NextIdle | RestIdle],
                                               in_flight = InFlight}}) ->
    NewInflight = [Message | InFlight],
    NewState =
        State#dqueue_db_state{db_store_state =
                                  #db_state{idle_consumers = RestIdle, in_flight = NewInflight}},
    NextIdle ! Message,
    {reply, ok, NewState};
handle_call({send_idle, _Message},
            _Ref,
            State = #dqueue_db_state{db_store_state = #db_state{idle_consumers = []}}) ->
    %% might need to remove from InFlight
    {reply, dropped, State}.

handle_cast(_, _) ->
    ok.

%% Client APIs
%%

add_idle_consumer(Pid) ->
    gen_server:call(?MODULE, {idle_consumers, add, Pid}).

remove_idle_consumer(Pid) ->
    gen_server:call(?MODULE, {idle_consumers, remove, Pid}).

add_inflight_message(Message) ->
    gen_server:call(?MODULE, {in_flight, add, Message}).

remove_inflight_message(Message) ->
    gen_server:call(?MODULE, {in_flight, remove, Message}).

send_if_any_idle(Message) ->
    gen_server:call(?MODULE, {send_idle, Message}).

-module(cqueue).

-export([handle_call/3, handle_cast/2, init/1, start_link/1, publish/1, acknowledge/1,
         consumer_loop/0, start_consumer/0, get/0]).

-behavior(gen_server).

-record(queue_state, {name, ets_table, in_flight = [], idle_consumers = []}).

-define(QUEUE_ETS_STORE_NAME, queue_store).

start_link(Name) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Name, []).

%% how to handle name
init(Name) ->
    Table = create_table_if_not_exists(),
    {ok,
     #queue_state{name = Name,
                  ets_table = Table,
                  in_flight = []}}.

create_table_if_not_exists() ->
    case lists:member(?QUEUE_ETS_STORE_NAME, ets:all()) of
        true ->
            ?QUEUE_ETS_STORE_NAME;
        false ->
            ets:new(?QUEUE_ETS_STORE_NAME, [ordered_set, named_table])
    end.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({publish, {_H, Message}},
            State =
                #queue_state{name = Name,
                             ets_table = EtsTable,
                             idle_consumers = []}) ->
    case ets:insert_new(EtsTable, {{Name, erlang:system_time()}, {_H, Message}}) of
        true ->
            {noreply, State};
        false ->
            {noreply,
             State} %% how to handle errors?. Might not be possible with handle_cast (for now)
    end;
handle_cast({publish, {_H, Message}},
            State = #queue_state{idle_consumers = [NextConsumer | _Rest], in_flight = InFlight}) ->
    Msg = {erlang:system_time(), {_H, Message}},
    NextConsumer ! Msg,
    {noreply, State#queue_state{idle_consumers = _Rest, in_flight = [Msg | InFlight]}};
handle_cast({get, ConsumerRef},
            State =
                #queue_state{name = Name,
                             ets_table = EtsTable,
                             in_flight = InFlight,
                             idle_consumers = IdleConsumers}) ->
    %% start consumer
    NewState =
        case ets:match_object(EtsTable, {{Name, '_'}, '_'}, 1) of
            {[{{_, MTs}, MMessage}], _} ->
                ets:delete_object(EtsTable, {{Name, MTs}, MMessage}),
                Msg1 = {MTs, MMessage},
                ConsumerRef ! Msg1,
                State#queue_state{in_flight = [Msg1 | InFlight]};
            '$end_of_table' ->
                State#queue_state{idle_consumers = [ConsumerRef | IdleConsumers]}
        end,
    {noreply, NewState};
handle_cast({acknowledge, {Ts, _}}, State = #queue_state{in_flight = InFlight}) ->
    NewInFlight =
        [{InflightMTs, InflightContent}
         || {InflightMTs, InflightContent} <- InFlight, InflightMTs =/= Ts],
    {noreply, State#queue_state{in_flight = NewInFlight}}.

%% Client API
get() ->
    %% handle case wehere Module is not started
    gen_server:cast(?MODULE, {get, self()}).

acknowledge({Ts, _Content}) ->
    gen_server:cast(?MODULE, {acknowledge, {Ts, _Content}}).

publish({Header, Message}) ->
    gen_server:cast(?MODULE, {publish, {Header, Message}}),
    ok.

%% Consumer API

start_consumer() ->
    Pid = spawn(cqueue, consumer_loop, []),
    {ok, Pid}.

consumer_loop() ->
    io:format("[CONSUMER] Waiting for new messages...\n"),
    cqueue:get(),
    receive
        Message ->
            io:format("[CONSUMER] Recived ~p\ Sending ACK\n", [Message]),
            cqueue:acknowledge(Message),
            consumer_loop()
    end.

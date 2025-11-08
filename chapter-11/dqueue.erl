-module(dqueue).

-export([handle_call/3, handle_cast/2, init/1, start_link/1, publish/1, acknowledge/1,
         get/0]).

-behavior(gen_server).

-record(queue_state, {name, db}).

-define(QUEUE_ETS_STORE_NAME, queue_store).

start_link(Name) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Name, []).

%% how to handle name
init(Name) ->
    {ok, DBPid} = dqueue_db:start_database(Name),
    dqueue_msg_store:start_database(Name),
    {ok, #queue_state{name = Name, db = DBPid}}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({publish, Msg = {_Ts, _H, _Message}}, State) ->
    PossiblySent = dqueue_db:send_if_any_idle(Msg),
    case PossiblySent of
        ok ->
            {noreply, State};
        dropped ->
            dqueue_msg_store:write(Msg),
            {noreply, State}
    end;
handle_cast({get, ConsumerRef}, State) ->
    PossiblyNextMessage = dqueue_msg_store:find_next_message(),
    case PossiblyNextMessage of
        {ok, '$end_of_table'} ->
            dqueue_db:add_idle_consumer(ConsumerRef),
            {noreply, State};
        {ok, Message} ->
            dqueue_db:add_inflight_message(Message),
            ConsumerRef ! Message,
            {noreply, State}
    end.

%% Client API
get() ->
    case whereis(?MODULE) of
        undefined ->
            {error, not_initialized};
        _Pid ->
            gen_server:cast(?MODULE, {get, self()})
    end.

acknowledge({Ts, _Content}) ->
    gen_server:cast(?MODULE, {acknowledge, {Ts, _Content}}).

publish({Header, Message}) ->
    gen_server:cast(?MODULE, {publish, {erlang:system_time(), Header, Message}}),
    ok.

-module(dqueue_msg_store).

-behaviour(gen_server).

-export([handle_call/3, handle_cast/2, init/1, start_database/1, write/1,
         find_next_message/0]).

-record(dqueue_msg_store, {ts_identifier, header, content}).

-define(QUEUE_STORE_NAME, dqueue_msg_store).

start_database(QueueName) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, QueueName, []).

init(_) ->
    mnesia:start(),
    application:ensure_all_started(mnesia),
    {ok, _} = create_table_if_not_exists(?QUEUE_STORE_NAME),
    {ok, []}.

create_table_if_not_exists(Name) ->
    case mnesia:create_table(?QUEUE_STORE_NAME,
                             [{ram_copies, [node() | nodes()]},
                              {record_name, dqueue_msg_store},
                              {type, set},
                              {attributes, record_info(fields, dqueue_msg_store)}])
    of
        {atomic, ok} ->
            io:format("Table ~p created successfully.~n", [Name]),
            {ok, Name};
        {aborted, {already_exists, _}} ->
            io:format("Table ~p already exists.~n", [Name]),
            {ok, Name};
        {aborted, Reason} ->
            io:format("Error creating table ~p: ~p~n", [Name, Reason])
    end.

handle_call({findNext}, _CRef, _State) ->
    Take =
        fun() ->
           case mnesia:first(?QUEUE_STORE_NAME) of
               '$end_of_table' -> '$end_of_table';
               NextKey ->
                   [{?QUEUE_STORE_NAME, Ts, H, Content}] = mnesia:read(dqueue_msg_store, NextKey),
                   mnesia:delete(?QUEUE_STORE_NAME, NextKey, write),
                   {Ts, H, Content}
           end
        end,
    {atomic, Response} = mnesia:transaction(Take),
    {reply, Response, _State}.

handle_cast({write, {Ts, Header, Content}}, _) ->
    ok =
        mnesia:dirty_write(?QUEUE_STORE_NAME,
                           #dqueue_msg_store{ts_identifier = Ts,
                                             header = Header,
                                             content = Content}),
    {noreply, []};
handle_cast({delete, Ts}, _) ->
    ok = mnesia:delete(?QUEUE_STORE_NAME, Ts),
    {noreply, []}.

write(Message) ->
    gen_server:cast(?MODULE, {write, Message}).

find_next_message() ->
    Response = gen_server:call(?MODULE, {findNext}),
    {ok, Response}.

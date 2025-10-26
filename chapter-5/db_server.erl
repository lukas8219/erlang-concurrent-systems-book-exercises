-module(db_server).

-export([start/0, stop/0, write/2, read/1, match/1, loop/1]).

start() ->
    Db = db:new(?MODULE_STRING),
    Pid = spawn(?MODULE, loop, [Db]),
    register(db_server, Pid),
    {ok, Pid}.

loop(State) ->
    receive
        {call, {write, Key, Element}, CalleePid} ->
            NewState = handle({write, Key, Element, CalleePid}, State),
            loop(NewState);
        {call, {read, Key}, CalleePid} ->
            handle({read, Key, CalleePid}, State),
            loop(State);
        {call, {match, Element}, CalleePid} ->
            handle({match, Element, CalleePid}, State),
            loop(State)
    end.

stop() ->
    ok.

handle({write, Key, Element, CalleePid}, State) ->
    NewState = db:write(Key, Element, State),
    CalleePid ! ok,
    NewState;
handle({read, Key, CalleePid}, State) ->
    {ok, Response} = db:read(Key, State),
    CalleePid ! {ok, Response};
handle({match, Element, CalleePid}, State) ->
    Response = db:match(Element, State),
    CalleePid ! {ok, Response}.

call({write, Key, Element}) ->
    emit({write, Key, Element});
call({read, Key}) ->
    emit({read, Key});
call({match, Element}) ->
    emit({match, Element}).

%% Client Operations
write(Key, Element) ->
    call({write, Key, Element}).

read(Key) ->
    call({read, Key}).

match(Element) ->
    call({match, Element}).

emit(Args) ->
    db_server ! {call, Args, self()},
    receive
        {ok, Response} ->
            {ok, Response};
        ok ->
            ok
    end.

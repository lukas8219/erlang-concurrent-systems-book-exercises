-module(echo_server).

-export([start/0, print/1, stop/0, loop/0]).

start() ->
    Pid = spawn(?MODULE, loop, []),
    register(echo_server, Pid),
    Pid.

loop() ->
    receive
        {message, Content} ->
            io:format("~w\n", [Content]),
            loop();
        {stop} ->
            ok
    end.

print(Message) ->
    echo_server ! {message, Message},
    ok.

stop() ->
    echo_server ! {stop},
    ok.

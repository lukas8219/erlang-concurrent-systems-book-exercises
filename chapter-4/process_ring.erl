-module(process_ring).

-export([start/3, loop/1]).

-record(state, {seq_no, next_process_pid, max_ring_size}).

start(MessagesPrinted, NumberOfProcesses, Message) ->
    Pid = spawn(?MODULE, loop, [#state{seq_no = 0, max_ring_size = NumberOfProcesses}]),
    register(process_ring, Pid),
    process_ring ! {print, MessagesPrinted, Message},
    Pid.

start_child(State = #state{seq_no = SeqNo, max_ring_size = MaxRingSize})
    when SeqNo < MaxRingSize ->
    spawn(?MODULE, loop, [State]).

loop(State =
         #state{seq_no = SeqNo,
                max_ring_size = MaxRingSize,
                next_process_pid = undefined})
    when SeqNo < MaxRingSize ->
    NewSeqNo = SeqNo + 1,
    NextState = State#state{seq_no = NewSeqNo},
    NextPid =
        case NewSeqNo >= MaxRingSize of
            true ->
                process_ring ! {ready},
                whereis(process_ring);
            false ->
                start_child(NextState)
        end,
    loop(State#state{next_process_pid = NextPid});
loop(State = #state{seq_no = 0, next_process_pid = NextPid}) when is_pid(NextPid) ->
    idle(State);
loop(State = #state{next_process_pid = NextPid}) when is_pid(NextPid) ->
    ready(State).

idle(State) ->
    receive
        {ready} ->
            ready(State)
    end.

ready(State = #state{next_process_pid = NextPid, seq_no = SeqNo}) when is_pid(NextPid) ->
    receive
        {print, 0, _} ->
            ok;
        {print, Count, Message} ->
            io:format("[SeqNo#~w][~w] ~p to [NextPid#~w]", [SeqNo, Count, Message, NextPid]),
            NextPid ! {print, Count - 1, Message},
            ready(State)
    end.

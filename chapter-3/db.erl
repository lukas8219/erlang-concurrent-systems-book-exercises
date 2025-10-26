-module(db).

-export([new/1, destroy/2, write/3, read/2, match/2]).

-include("db.hrl").

new(Name) ->
    #db_state{name = Name, entries = []}.

destroy([], _State) ->
    ok;
destroy([_ | T], State) ->
    destroy(T, State).

write(Key, Entry, State = #db_state{entries = Entries}) ->
    NewEntries = [{Key, Entry} | Entries],
    NewState = State#db_state{entries = NewEntries},
    NewState.

read(Key, State = #db_state{entries = Entries}) ->
    FoundEntries =
        lists:filter(fun ({K, _}) when K =:= Key ->
                             true;
                         (_) ->
                             false
                     end,
                     Entries),
    case FoundEntries of
        [] ->
            {not_found, State};
        _ ->
            [Head | _] = FoundEntries,
            {_, Value} = Head,
            {ok, Value}
    end.

match(Value, #db_state{entries = Entries}) ->
    FoundMatchingEntries =
        lists:filter(fun ({_, V}) when V =:= Value ->
                             true;
                         (_) ->
                             false
                     end,
                     Entries),
    lists:map(fun({K, _}) -> K end, FoundMatchingEntries).

-module(sum).

-export([sum/1, sum/2, create/1, reverse_create/1]).

%% write a function that will sum all values like sum(5) -> 1 + 2 + 3 + 4 + 5 = 15
sum(0) ->
    0;
sum(Times) ->
    Times + sum(Times - 1).

sum(N, M) when N =< M ->
    N + sum(N + 1, M);
sum(N, M) when N > M ->
    0.

create(0) ->
    [];
create(N) when N > 0 ->
    create(N - 1) ++ [N].

reverse_create(0) ->
    [];
reverse_create(N) when N > 0 ->
    [N | reverse_create(N - 1)].

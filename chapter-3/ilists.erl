-module(ilists).

-export([filter/2, reverse/1, concatenate/1]).

filter(_, Amount) when Amount =:= 0 ->
    [];
filter(ListOfIntegers, Amount) ->
    [T | Head] = ListOfIntegers,
    [T] ++ filter(Head, Amount - 1).

reverse([]) ->
    [];
reverse(ListOfItems) ->
    [T | H] = ListOfItems,
    reverse(H) ++ [T].

concatenate(ListOfLists) ->
    [T | Head] = ListOfLists,
    concatenate(T, Head).

concatenate(Acc, []) ->
    Acc;
concatenate(Acc, ListOfLists) ->
    [T | Head] = ListOfLists,
    NewAcc = Acc ++ T,
    concatenate(NewAcc, Head).

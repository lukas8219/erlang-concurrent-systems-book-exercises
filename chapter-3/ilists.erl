-module(ilists).

-export([filter/2, reverse/1, concatenate/1, flatten/1, merge_sort/1, main/1]).

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

concatenate([]) ->
    [];
concatenate(ListOfLists) ->
    [T | Head] = ListOfLists,
    concatenate(T, Head).

concatenate(Acc, []) ->
    Acc;
concatenate(Acc, ListOfLists) ->
    [T | Head] = ListOfLists,
    NewAcc = Acc ++ T,
    concatenate(NewAcc, Head).

flatten([]) ->
    [];
flatten(ListOfLists) when is_list(ListOfLists) ->
    [T | Head] = ListOfLists,
    FlattenedTail = flatten(T),
    FlattenedHead = flatten(Head),
    concatenate([FlattenedTail, FlattenedHead]);
flatten(X) ->
    [X].

merge_sort(Array) ->
    Nodes = build_left_and_right_nodes(Array),
    merge_left_and_right(Nodes).

build_left_and_right_nodes([A]) ->
    [A];
build_left_and_right_nodes([A, B]) when A >= B ->
    [B, A];
build_left_and_right_nodes([A, B]) ->
    [A, B];
build_left_and_right_nodes(Array) when is_list(Array), length(Array) > 2 ->
    Half = length(Array) div 2,
    {Left, Right} = lists:split(Half, Array),
    LeftNodes = build_left_and_right_nodes(Left),
    RightNodes = build_left_and_right_nodes(Right),
    {LeftNodes, RightNodes}.

merge_left_and_right(Nodes) when is_list(Nodes) ->
    Nodes;
merge_left_and_right({LeftNode, RightNode}) ->
    LeftTree = merge_left_and_right(LeftNode),
    RightTree = merge_left_and_right(RightNode),
    merge(LeftTree, RightTree).

merge([], X) ->
    X;
merge(X, []) ->
    X;
merge(LeftNodes, RightNodes) ->
    [LHead | LTail] = LeftNodes,
    [RHead | RTail] = RightNodes,
    %% Consume consume entire Left nodes
    case LHead < RHead of
        true ->
            [LHead | merge(LTail, RightNodes)];
        false ->
            [RHead | merge(LeftNodes, RTail)]
    end.

main(_) ->
    merge_sort([6, 5, 4, 3]).

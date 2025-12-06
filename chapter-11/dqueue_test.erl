-module(dqueue_test).

-include_lib("eunit/include/eunit.hrl").

%% Test fixtures
dqueue_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun test_publish_and_consume/1,
      fun test_multiple_publish_and_consume/1,
      fun test_consume_with_no_messages/1,
      fun test_publish_to_idle_consumer/1,
      fun test_multiple_consumers/1]}.

%% Setup and cleanup
setup() ->
    %% Start the distributed queue system
    {ok, _Pid} = dqueue:start_link(test_queue),
    %% Give it time to initialize
    timer:sleep(100),
    ok.

cleanup(_) ->
    catch gen_server:stop(dqueue, normal, 5000),
    catch gen_server:stop(dqueue_db, normal, 5000),
    catch gen_server:stop(dqueue_msg_store, normal, 5000),
    mnesia:clear_table(dqueue_msg_store),
    ok.

test_publish_and_consume(_) ->
    ok = dqueue:publish({header1, <<"test message">>}),
    timer:sleep(50),

    ok = dqueue:get(),

    Message =
        receive
            {Ts, header1, <<"test message">>} = Msg -> Msg,
            ?_assertMatch({Ts, header1, <<"test message">>}, Msg)
        after 1000 -> timeout
        end,

    [?_assertNotEqual(timeout, Message)].

test_multiple_publish_and_consume(_) ->
    %% Publish multiple messages
    ok = dqueue:publish({header1, <<"message 1">>}),
    ok = dqueue:publish({header2, <<"message 2">>}),
    ok = dqueue:publish({header3, <<"message 3">>}),
    timer:sleep(50),

    %% Consume all messages
    ok = dqueue:get(),
    Msg1 =
        receive
            {_Ts1, _H1, _C1} = M1 -> M1
        after 1000 -> timeout
        end,

    ok = dqueue:get(),
    Msg2 =
        receive
            {_Ts2, _H2, _C2} = M2 -> M2
        after 1000 -> timeout
        end,

    ok = dqueue:get(),
    Msg3 =
        receive
            {_Ts3, _H3, _C3} = M3 -> M3
        after 1000 -> timeout
        end,

    [?_assertNotEqual(timeout, Msg1),
     ?_assertNotEqual(timeout, Msg2),
     ?_assertNotEqual(timeout, Msg3),
     ?_assertEqual(3, length([Msg1, Msg2, Msg3]))].

test_consume_with_no_messages(_) ->
    %% Try to consume when there are no messages
    %% The consumer should be added to idle consumers
    ok = dqueue:get(),

    %% Wait a bit to ensure no message arrives
    NoMessage =
        receive
            {_Ts, _H, _C} = Msg -> Msg
        after 500 -> no_message_received
        end,

    [?_assertEqual(no_message_received, NoMessage)].

test_publish_to_idle_consumer(_) ->
    %% First, register as an idle consumer
    ok = dqueue:get(),
    timer:sleep(50),

    %% Now publish a message - it should go directly to the idle consumer
    ok = dqueue:publish({direct_header, <<"direct message">>}),

    %% Receive the message immediately
    Message =
        receive
            {Ts, direct_header, <<"direct message">>} = Msg -> Msg,
            ?_assertMatch({Ts, direct_header, <<"direct message">>}, Msg)
        after 1000 -> timeout
        end,

    [?_assertNotEqual(timeout, Message)].

test_multiple_consumers(_) ->
    %% Spawn multiple consumer processes
    Parent = self(),

    Consumer1 =
        spawn(fun() ->
                      ok = dqueue:get(),
                      receive
                          {_Ts, _H, _C} = Msg -> Parent ! {consumer1, Msg}
                      after 2000 -> Parent ! {consumer1, timeout}
                      end
              end),

    Consumer2 =
        spawn(fun() ->
                      ok = dqueue:get(),
                      receive
                          {_Ts, _H, _C} = Msg -> Parent ! {consumer2, Msg}
                      after 2000 -> Parent ! {consumer2, timeout}
                      end
              end),

    timer:sleep(100),

    %% Publish two messages
    ok = dqueue:publish({multi1, <<"message for consumer 1">>}),
    ok = dqueue:publish({multi2, <<"message for consumer 2">>}),

    %% Collect results from both consumers
    Results =
        [receive
             {ConsumerName, Msg} -> {ConsumerName, Msg}
         after 2000 -> timeout
         end
         || _ <- [Consumer1, Consumer2]],

    %% Both consumers should have received a message
    [?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun({_, timeout}) -> false;
                           ({_, _}) -> true
                        end,
                        Results))].

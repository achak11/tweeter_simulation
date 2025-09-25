-module(tweeter).
-define(ZIPF_POWER, 2).
-define(SUB2TWEETS, 1).
-define(TWEET_HASHTAG_LIM, 3).
-define(TWEET_MENTIONS_LIM, 3).
-define(QUERY_HASHTAGS_LIM, 3).
-define(QUERY_MENTIONS_LIM, 3).

-export([
    start_server/0,
    start_client/2,
    start_simulation/3
]).

start_server() ->
    register(server, self()),
    io:format("[Server] Starting\n", []),
    server_listen([], [], [], #{}, #{}, #{}, #{}),
    unregister(server).

server_listen(
    Tweets,
    Users,
    LiveUsers,
    UserToTweetMap,
    HashTagToTweetMap,
    MentionsToTweetMap,
    UserToSubscriberMap
) ->
    receive
        {exit} ->
            io:format("[Server] Invoking exit\n");
        {register, From, Username} ->
            io:format("[Server] Received registration request from ~p\n", [Username]),
            case lists:member(Username, Users) of
                true ->
                    send_message(From, {register, failed, "Username already exists."}),
                    server_listen(
                        Tweets,
                        Users,
                        LiveUsers,
                        UserToTweetMap,
                        HashTagToTweetMap,
                        MentionsToTweetMap,
                        UserToSubscriberMap
                    );
                false ->
                    NewUsers = Users ++ [Username],
                    send_message(From, {register, success, "Username created successfully."}),
                    server_listen(
                        Tweets,
                        NewUsers,
                        LiveUsers,
                        UserToTweetMap,
                        HashTagToTweetMap,
                        MentionsToTweetMap,
                        UserToSubscriberMap
                    )
            end;
        {connect, From, Username, RequestStartTime} ->
            io:format("[Server] Received connection request from ~p\n", [Username]),

            case lists:member(Username, Users) of
                true ->
                    case lists:keymember(Username, 1, LiveUsers) of
                        true ->
                            send_message(
                                From,
                                {connect, success, "Username is already live.", RequestStartTime}
                            ),
                            server_listen(
                                Tweets,
                                Users,
                                LiveUsers,
                                UserToTweetMap,
                                HashTagToTweetMap,
                                MentionsToTweetMap,
                                UserToSubscriberMap
                            );
                        false ->
                            send_message(
                                From, {connect, success, "Username is live.", RequestStartTime}
                            ),
                            NewLiveUsers = LiveUsers ++ [{Username, From}],
                            server_listen(
                                Tweets,
                                Users,
                                NewLiveUsers,
                                UserToTweetMap,
                                HashTagToTweetMap,
                                MentionsToTweetMap,
                                UserToSubscriberMap
                            )
                    end;
                false ->
                    send_message(
                        From,
                        {connect, failed, "Username does not exist, failed to connect.",
                            RequestStartTime}
                    ),
                    server_listen(
                        Tweets,
                        Users,
                        LiveUsers,
                        UserToTweetMap,
                        HashTagToTweetMap,
                        MentionsToTweetMap,
                        UserToSubscriberMap
                    )
            end;
        {disconnect, From, Username} ->
            case lists:member(Username, Users) of
                true ->
                    case lists:keymember(Username, 1, LiveUsers) of
                        true ->
                            NewLiveUsers = lists:keydelete(Username, 1, LiveUsers),
                            send_message(
                                From, {disconnect, success, "Username has been disconnected."}
                            ),
                            server_listen(
                                Tweets,
                                Users,
                                NewLiveUsers,
                                UserToTweetMap,
                                HashTagToTweetMap,
                                MentionsToTweetMap,
                                UserToSubscriberMap
                            );
                        false ->
                            send_message(
                                From, {disconnect, success, "Username is already disconnected"}
                            ),
                            server_listen(
                                Tweets,
                                Users,
                                LiveUsers,
                                UserToTweetMap,
                                HashTagToTweetMap,
                                MentionsToTweetMap,
                                UserToSubscriberMap
                            )
                    end;
                false ->
                    send_message(
                        From, {disconnect, failed, "Username does not exist, failed to disconnect."}
                    ),
                    server_listen(
                        Tweets,
                        Users,
                        LiveUsers,
                        UserToTweetMap,
                        HashTagToTweetMap,
                        MentionsToTweetMap,
                        UserToSubscriberMap
                    )
            end;
        {tweet, From, Username, Message, RequestStartTime} ->
            NewTweets = Tweets ++ [Message],
            NewUserToTweetMap = maps:update_with(
                Username,
                fun(V) -> [length(NewTweets)] ++ V end,
                [length(NewTweets)],
                UserToTweetMap
            ),
            {HashTags, Mentions} = extract_hashtags_and_mentions(Message),
            NewHashTagToTweetMap = update_hashtags(HashTags, HashTagToTweetMap, length(NewTweets)),
            NewMentionsToTweetMap = update_mentions(
                Mentions, MentionsToTweetMap, length(NewTweets)
            ),

            send_message(From, {tweet, success, "Tweet posted successfully.", RequestStartTime}),

            Subscribers = maps:get(Username, UserToSubscriberMap, []),
            NewSubscribers = remove_elements_from_list(Subscribers, Mentions),
            LiveSubscribersPid = get_live_users(NewSubscribers, LiveUsers, []),
            LiveMentionsPid = get_live_users(Mentions, LiveUsers, []),
            lists:foreach(
                fun(LiveSubscriberPid) ->
                    send_message(LiveSubscriberPid, {new_tweet_from_subscriber, Message, Username})
                end,
                LiveSubscribersPid
            ),
            lists:foreach(
                fun(LiveMentionPid) ->
                    send_message(LiveMentionPid, {new_mention, Message, Username})
                end,
                LiveMentionsPid
            ),

            server_listen(
                NewTweets,
                Users,
                LiveUsers,
                NewUserToTweetMap,
                NewHashTagToTweetMap,
                NewMentionsToTweetMap,
                UserToSubscriberMap
            );
        {retweet, From, Username, TweetGobalIndex, RequestStartTime} ->
            Message = lists:nth(TweetGobalIndex, Tweets),
            NewTweets = Tweets ++ [Message],
            NewUserToTweetMap = maps:update_with(
                Username,
                fun(V) -> [length(NewTweets)] ++ V end,
                [length(NewTweets)],
                UserToTweetMap
            ),

            send_message(
                From, {retweet, success, "Re-tweet posted successfully.", RequestStartTime}
            ),
            {_, Mentions} = extract_hashtags_and_mentions(Message),
            Subscribers = maps:get(Username, UserToSubscriberMap, []),
            NewSubscribers = remove_elements_from_list(Subscribers, Mentions),
            LiveSubscribersPid = get_live_users(NewSubscribers, LiveUsers, []),
            LiveMentionsPid = get_live_users(Mentions, LiveUsers, []),
            lists:foreach(
                fun(LiveSubscriberPid) ->
                    send_message(LiveSubscriberPid, {new_tweet_from_subscriber, Message, Username})
                end,
                LiveSubscribersPid
            ),
            lists:foreach(
                fun(LiveMentionPid) ->
                    send_message(LiveMentionPid, {new_mention, Message, Username})
                end,
                LiveMentionsPid
            ),

            server_listen(
                NewTweets,
                Users,
                LiveUsers,
                NewUserToTweetMap,
                HashTagToTweetMap,
                MentionsToTweetMap,
                UserToSubscriberMap
            );
        {follow, From, Username, ToFollowUsername} ->
            case maps:get(Username, UserToSubscriberMap, default) of
                default ->
                    NewSubscribers = [ToFollowUsername],
                    send_message(From, {follow, success, "User has been added to followers list."});
                Subscribers ->
                    case lists:member(ToFollowUsername, Subscribers) of
                        true ->
                            send_message(From, {follow, success, "Already following user."}),
                            NewSubscribers = Subscribers;
                        false ->
                            send_message(
                                From, {follow, success, "User has been added to followers list."}
                            ),
                            NewSubscribers = Subscribers ++ [ToFollowUsername]
                    end
            end,
            NewUserToSubscriberMap = maps:put(Username, NewSubscribers, UserToSubscriberMap),
            server_listen(
                Tweets,
                Users,
                LiveUsers,
                UserToTweetMap,
                HashTagToTweetMap,
                MentionsToTweetMap,
                NewUserToSubscriberMap
            );
        {query, From, QuerySubscribedTo, QueryHashtags, QueryMentions, RequestStartTime} ->
            if
                QuerySubscribedTo == [] ->
                    QuerySubscribedToIndexes = [];
                true ->
                    QuerySubscribedToIndexes = get_query_results(
                        UserToTweetMap, QuerySubscribedTo, []
                    )
            end,
            if
                QueryHashtags == [] ->
                    QueryHashtagsIndexes = [];
                true ->
                    QueryHashtagsIndexes = get_query_results(HashTagToTweetMap, QueryHashtags, [])
            end,
            if
                QueryMentions == [] ->
                    QueryMentionsIndexes = [];
                true ->
                    QueryMentionsIndexes = get_query_results(MentionsToTweetMap, QueryMentions, [])
            end,
            QueryResultIndexes = lists:uniq(
                QuerySubscribedToIndexes ++ QueryHashtagsIndexes ++ QueryMentionsIndexes
            ),
            QueriedTweets = get_tweets_from_indexes(Tweets, QueryResultIndexes, []),
            send_message(From, {queried_tweets, QueriedTweets, RequestStartTime}),
            server_listen(
                Tweets,
                Users,
                LiveUsers,
                UserToTweetMap,
                HashTagToTweetMap,
                MentionsToTweetMap,
                UserToSubscriberMap
            )
    end.

get_tweets_from_indexes(_, [], TweetsToReturn) ->
    TweetsToReturn;
get_tweets_from_indexes(AllTweets, [Index | Indexes], TweetsToReturn) ->
    get_tweets_from_indexes(
        AllTweets, Indexes, TweetsToReturn ++ [{Index, lists:nth(Index, AllTweets)}]
    ).

get_query_results(_, [], QueryResult) ->
    QueryResult;
get_query_results(QueryMap, [Query | Queries], QueryResult) ->
    case maps:get(Query, QueryMap, default) of
        default ->
            get_query_results(QueryMap, Queries, QueryResult);
        Value ->
            get_query_results(QueryMap, Queries, Value ++ QueryResult)
    end.

get_live_users([], _, LiveUsersResult) ->
    LiveUsersResult;
get_live_users([Elem | List], LiveUsers, LiveUsersResult) ->
    case lists:keyfind(Elem, 1, LiveUsers) of
        false ->
            NewLiveUsersResult = LiveUsersResult;
        LiveUser ->
            NewLiveUsersResult = LiveUsersResult ++ [element(2, LiveUser)]
    end,
    get_live_users(List, LiveUsers, NewLiveUsersResult).

remove_elements_from_list([], _) ->
    [];
remove_elements_from_list(List1, []) ->
    List1;
remove_elements_from_list(List1, [Elem2 | List2]) ->
    NewList1 = lists:delete(Elem2, List1),
    remove_elements_from_list(NewList1, List2).

update_hashtags([], HashTagToTweetMap, _) ->
    HashTagToTweetMap;
update_hashtags([HashTag | HashTags], HashTagToTweetMap, Value) ->
    NewHashTagToTweetMap = maps:update_with(
        HashTag, fun(V) -> [Value] ++ V end, [Value], HashTagToTweetMap
    ),
    update_hashtags(HashTags, NewHashTagToTweetMap, Value).

update_mentions([], MentionsToTweetMap, _) ->
    MentionsToTweetMap;
update_mentions([Mention | Mentions], MentionsToTweetMap, Value) ->
    NewMentionsToTweetMap = maps:update_with(
        Mention, fun(V) -> [Value] ++ V end, [Value], MentionsToTweetMap
    ),
    update_mentions(Mentions, NewMentionsToTweetMap, Value).

extract_hashtags_and_mentions(Message) ->
    TokenizedMessage = string:lexemes(Message, " "),
    iterate_message(TokenizedMessage, [], []).

iterate_message([], HashTags, Mentions) ->
    {HashTags, Mentions};
iterate_message([Token | TokenizedMessage], HashTags, Mentions) ->
    FirstChar = string:slice(Token, 0, 1),
    case FirstChar of
        "#" ->
            NewHashTags = [string:slice(Token, 1)] ++ HashTags,
            iterate_message(TokenizedMessage, NewHashTags, Mentions);
        "@" ->
            NewMentions = [string:slice(Token, 1)] ++ Mentions,
            iterate_message(TokenizedMessage, HashTags, NewMentions);
        _ ->
            iterate_message(TokenizedMessage, HashTags, Mentions)
    end.

send_message(SendTo, Message) ->
    SendTo ! Message.

start_client(Username, SimulatorPid) ->
    % register username
    send_message(server, {register, self(), Username}),
    receive
        {register, success, Message} ->
            io:format("[~p] Successfully registered, message from server: ~p\n", [Username, Message]),
            client_listen(Username, SimulatorPid, #{
                connect => {0, 0}, tweet => {0, 0}, retweet => {0, 0}, query => {0, 0}
            });
        {register, failed, Message} ->
            io:format("[~p] Failed to register, message from server: ~p\n", [Username, Message])
    end.

client_listen(Username, SimulatorPid, RequestData) ->
    receive
        {exit} ->
            io:format("[~p] Exit command invoked, exiting.\n", [Username]);
        {send_connect} ->
            io:format("[~p] Send connect invoked.\n", [Username]),

            RequestStartTime = erlang:monotonic_time(nanosecond),
            send_message(server, {connect, self(), Username, RequestStartTime}),
            client_listen(Username, SimulatorPid, RequestData);
        {connect, success, Message, RequestStartTime} ->
            RequestEndTime = erlang:monotonic_time(nanosecond),
            RequestTime = RequestEndTime - RequestStartTime,
            {PreviousRequestTotalTime, PreviousRequestCount} = maps:get(connect, RequestData),
            NewRequestData = maps:put(
                connect,
                {PreviousRequestTotalTime + RequestTime, PreviousRequestCount + 1},
                RequestData
            ),
            io:format("[~p] Connected to server. Message from server: ~p\n", [Username, Message]),
            client_listen(Username, SimulatorPid, NewRequestData);
        {connect, failed, Message, _} ->
            %RequestEndTime = erlang:monotonic_time(nanosecond),
            %RequestTime = RequestEndTime - RequestStartTime,
            %{PreviousRequestTotalTime, PreviousRequestCount} = maps:get(connect, RequestData),
            % maps:put(
            %     connect,
            %     {PreviousRequestTotalTime + RequestTime, PreviousRequestCount + 1},
            %     RequestData
            % ),
            io:format("[~p] Connection to server failed, message from server: ~p\n", [
                Username, Message
            ]);
        {send_disconnect} ->
            io:format("[~p] Send disconnect invoked.\n", [Username]),
            send_message(server, {disconnect, self(), Username}),
            client_listen(Username, SimulatorPid, RequestData);
        {disconnect, success, Message} ->
            io:format("[~p] Disconnected from server. Message from server: ~p\n", [
                Username, Message
            ]),
            client_listen(Username, SimulatorPid, RequestData);
        {disconnect, failed, Message} ->
            io:format("[~p] Disconnection to server failed, message from server: ~p\n", [
                Username, Message
            ]);
        {add_follower, ToFollowUsername} ->
            io:format("[~p] Follow ~p invoked.\n", [Username, ToFollowUsername]),
            send_message(server, {follow, self(), Username, ToFollowUsername}),
            client_listen(Username, SimulatorPid, RequestData);
        {follow, success, Message} ->
            io:format("[~p] Follow success from Server. Message from server: ~p\n", [
                Username, Message
            ]),
            client_listen(Username, SimulatorPid, RequestData);
        {send_tweet, Message} ->
            io:format("[~p] Tweet \"~s\" invoked.\n", [Username, Message]),
            RequestStartTime = erlang:monotonic_time(nanosecond),
            send_message(server, {tweet, self(), Username, Message, RequestStartTime}),
            client_listen(Username, SimulatorPid, RequestData);
        {tweet, success, Message, RequestStartTime} ->
            RequestEndTime = erlang:monotonic_time(nanosecond),
            RequestTime = RequestEndTime - RequestStartTime,
            {PreviousRequestTotalTime, PreviousRequestCount} = maps:get(connect, RequestData),
            NewRequestData = maps:put(
                tweet,
                {PreviousRequestTotalTime + RequestTime, PreviousRequestCount + 1},
                RequestData
            ),

            io:format("[~p] Tweet successful, message from server: ~p\n", [Username, Message]),
            client_listen(Username, SimulatorPid, NewRequestData);
        {retweet, TweetGobalIndex} ->
            io:format("[~p] Re-tweet with index ~p invoked.\n", [Username, TweetGobalIndex]),
            RequestStartTime = erlang:monotonic_time(nanosecond),
            send_message(server, {retweet, self(), Username, TweetGobalIndex, RequestStartTime}),
            client_listen(Username, SimulatorPid, RequestData);
        {retweet, success, Message, RequestStartTime} ->
            RequestEndTime = erlang:monotonic_time(nanosecond),
            RequestTime = RequestEndTime - RequestStartTime,
            {PreviousRequestTotalTime, PreviousRequestCount} = maps:get(connect, RequestData),
            NewRequestData = maps:put(
                retweet,
                {PreviousRequestTotalTime + RequestTime, PreviousRequestCount + 1},
                RequestData
            ),
            io:format("[~p] Re-tweet successful, message from server: ~p\n", [Username, Message]),
            send_message(SimulatorPid, Message),
            client_listen(Username, SimulatorPid, NewRequestData);
        {new_tweet_from_subscriber, Message, SubscribedUsername} ->
            io:format("[~p] [Live Update] New tweet from subscriber ~p: \"~s\"\n", [
                Username, SubscribedUsername, Message
            ]),
            client_listen(Username, SimulatorPid, RequestData);
        {new_mention, Message, MentionedBy} ->
            io:format("[~p] [Live Update] New mention from user ~p on tweet \"~s\"\n", [
                Username, MentionedBy, Message
            ]),
            client_listen(Username, SimulatorPid, RequestData);
        {query, QuerySubscribedTo, QueryHashtags, QueryMentions} ->
            io:format("[~p] Query invoked with params: Users=~p, Hashtags=~p, Mentions=~p\n", [
                Username, QuerySubscribedTo, QueryHashtags, QueryMentions
            ]),
            RequestStartTime = erlang:monotonic_time(nanosecond),
            send_message(
                server,
                {query, self(), QuerySubscribedTo, QueryHashtags, QueryMentions, RequestStartTime}
            ),
            client_listen(Username, SimulatorPid, RequestData);
        {queried_tweets, QueriedTweets, RequestStartTime} ->
            % io:format("[~p] Query response from server: ~p\n", [Username, QueriedTweets]),
            RequestEndTime = erlang:monotonic_time(nanosecond),
            RequestTime = RequestEndTime - RequestStartTime,
            {PreviousRequestTotalTime, PreviousRequestCount} = maps:get(connect, RequestData),
            NewRequestData = maps:put(
                query,
                {PreviousRequestTotalTime + RequestTime, PreviousRequestCount + 1},
                RequestData
            ),
            send_message(SimulatorPid, {Username, QueriedTweets}),
            client_listen(Username, SimulatorPid, NewRequestData)
    end,
    send_message(SimulatorPid, {request_data, RequestData}).

start_simulation(Start, NumClients, NumRequests) ->
    io:format("[Simulator~w] Starting simulation for ~w Clients and ~w Requests\n", [
        Start, NumClients, NumRequests
    ]),
    spawn(tweeter, start_server, []),

    % Spawn Users
    UsersList = lists:map(
        fun(Client) ->
            Username = get_username(Client),
            UserPid = spawn(tweeter, start_client, [Username, self()]),
            {Username, UserPid}
        end,
        lists:seq(Start, Start + NumClients - 1)
    ),
    timer:sleep(1000 * trunc(NumClients / 1000)),
    % Connect Users
    lists:foreach(
        fun(User) ->
            send_message(element(2, User), {send_connect})
        end,
        UsersList
    ),

    % Follow Users
    UsersListWithSubCount = lists:map(
        fun(User) ->
            Subscribers = get_subscribers(Start, NumClients, element(1, User)),
            lists:foreach(
                fun(Subscriber) -> send_message(element(2, User), {add_follower, Subscriber}) end,
                Subscribers
            ),
            {element(1, User), element(2, User), length(Subscribers)}
        end,
        UsersList
    ),

    Tweets = read_file("loremip.txt"),
    HashTags = read_file("hashtags.txt"),
    StartTime = erlang:monotonic_time(seconds),
    statistics(wall_clock),
    simulation_loop(
        Start,
        NumClients,
        StartTime,
        NumRequests,
        0,
        UsersListWithSubCount,
        [],
        Tweets,
        HashTags
    ).

simulation_loop(
    Start,
    NumClients,
    StartTime,
    NumRequests,
    CurrentNumRequests,
    LiveUsersList,
    DisconnectedUsersList,
    Tweets,
    HashTags
) ->
    if
        CurrentNumRequests >= NumRequests ->
            AccRequestData = lists:foldl(
                fun(User, AccRequestData) ->
                    send_message(element(2, User), {exit}),
                    receive
                        {request_data, RequestData} ->
                            {ConnectTotalTime, TotalConnectCalls} = maps:get(
                                connect, AccRequestData
                            ),
                            {TweetTotalTime, TotalTweetCalls} = maps:get(
                                tweet, AccRequestData
                            ),
                            {RetweetTotalTime, TotalRetweetCalls} = maps:get(
                                retweet, AccRequestData
                            ),
                            {QueryTotalTime, TotalQueryCalls} = maps:get(query, AccRequestData),

                            {RequestConnectTotalTime, RequestTotalConnectCalls} = maps:get(
                                connect, RequestData
                            ),
                            {RequestTweetTotalTime, RequestTotalTweetCalls} = maps:get(
                                tweet, RequestData
                            ),
                            {RequestRetweetTotalTime, RequestTotalRetweetCalls} = maps:get(
                                retweet, RequestData
                            ),
                            {RequestQueryTotalTime, RequestTotalQueryCalls} = maps:get(
                                query, RequestData
                            ),
                            io:format(
                                "Connect time: ~w, Connect Requests: ~w for user ~p\n", [
                                    RequestConnectTotalTime,
                                    RequestTotalConnectCalls,
                                    element(1, User)
                                ]
                            ),
                            io:format(
                                "Tweet time: ~w, Tweet Requests: ~w for user ~p\n", [
                                    RequestTweetTotalTime, RequestTotalTweetCalls, element(1, User)
                                ]
                            ),
                            io:format(
                                "Retweet time: ~w, Retweet Requests: ~w for user ~p\n", [
                                    RequestRetweetTotalTime,
                                    RequestTotalRetweetCalls,
                                    element(1, User)
                                ]
                            ),
                            io:format(
                                "Query time: ~w, Query Requests: ~w for user ~p\n", [
                                    RequestQueryTotalTime, RequestTotalQueryCalls, element(1, User)
                                ]
                            ),

                            AccRequestData1 = maps:put(
                                connect,
                                {
                                    ConnectTotalTime + RequestConnectTotalTime,
                                    TotalConnectCalls + RequestTotalConnectCalls
                                },
                                AccRequestData
                            ),
                            AccRequestData2 = maps:put(
                                tweet,
                                {
                                    TweetTotalTime + RequestTweetTotalTime,
                                    TotalTweetCalls + RequestTotalTweetCalls
                                },
                                AccRequestData1
                            ),
                            AccRequestData3 = maps:put(
                                retweet,
                                {
                                    RetweetTotalTime + RequestRetweetTotalTime,
                                    TotalRetweetCalls + RequestTotalRetweetCalls
                                },
                                AccRequestData2
                            ),
                            AccRequestData4 = maps:put(
                                query,
                                {
                                    QueryTotalTime + RequestQueryTotalTime,
                                    TotalQueryCalls + RequestTotalQueryCalls
                                },
                                AccRequestData3
                            ),
                            AccRequestData4
                    end
                end,
                #{connect => {0, 0}, tweet => {0, 0}, retweet => {0, 0}, query => {0, 0}},
                LiveUsersList ++ DisconnectedUsersList
            ),

            % timer:sleep(1000),
            send_message(server, {exit}),
            {ConnectTotalTime, TotalConnectCalls} = maps:get(
                connect, AccRequestData
            ),
            {TweetTotalTime, TotalTweetCalls} = maps:get(tweet, AccRequestData),
            {RetweetTotalTime, TotalRetweetCalls} = maps:get(
                retweet, AccRequestData
            ),
            {QueryTotalTime, TotalQueryCalls} = maps:get(query, AccRequestData),

            AvgConnectTime = ConnectTotalTime / (TotalConnectCalls * NumClients),
            AvgTweetTime = TweetTotalTime / (TotalTweetCalls * NumClients),
            AvgRetweetTime = RetweetTotalTime / (TotalConnectCalls * NumClients),
            AvgQueryTime = QueryTotalTime / (TotalQueryCalls * NumClients),

            io:format("[Simulator~w] Total Users: ~w\n", [Start, NumClients]),
            io:format("[Simulator~w] Simulation total run time: ~w\n", [
                Start, element(2, statistics(wall_clock))
            ]),
            io:format(
                "[Simulator~w] Average Connect time: ~.2f microseconds, Total Connect Requests: ~w\n",
                [
                    Start, AvgConnectTime / 1000, TotalConnectCalls
                ]
            ),
            io:format(
                "[Simulator~w] Average Tweet time: ~.2f microseconds, Total Tweet Requests: ~w\n",
                [
                    Start, AvgTweetTime / 1000, TotalTweetCalls
                ]
            ),
            io:format(
                "[Simulator~w] Average Retweet time: ~.2f microseconds, Total Retweet Requests: ~w\n",
                [
                    Start, AvgRetweetTime / 1000, TotalRetweetCalls
                ]
            ),
            io:format(
                "[Simulator~w] Average Query time: ~.2f microseconds, Total Query Requests: ~w\n",
                [
                    Start, AvgQueryTime / 1000, TotalQueryCalls
                ]
            ),

            io:format("[Simulator~w] Exiting simulator as time limit reached.\n", [Start]),
            erlang:exit(self(), normal);
        true ->
            Command = get_command_to_invoke(),
            case Command of
                1 ->
                    % Send tweet
                    if
                        LiveUsersList == [] ->
                            simulation_loop(
                                Start,
                                NumClients,
                                StartTime,
                                NumRequests,
                                CurrentNumRequests,
                                LiveUsersList,
                                DisconnectedUsersList,
                                Tweets,
                                HashTags
                            );
                        true ->
                            {Username, ClientPid, NumSubscribers} = lists:nth(
                                rand:uniform(length(LiveUsersList)), LiveUsersList
                            ),
                            % Send NumSubs/SUB2TWEETS tweets
                            lists:foreach(
                                fun(_) ->
                                    io:format("[Simulator~w] Invoking send_tweet on ~p.\n", [
                                        Start, Username
                                    ]),
                                    TweetText = lists:nth(rand:uniform(length(Tweets)), Tweets),
                                    TweetWithMentionsAndHashtags =
                                        TweetText ++
                                            lists:map(
                                                fun(Mention) -> " @" ++ Mention end,
                                                sample_mentions(
                                                    Start, NumClients, ?TWEET_MENTIONS_LIM
                                                )
                                            ) ++
                                            lists:map(
                                                fun(HashTag) -> " #" ++ HashTag end,
                                                sample_hashtags(HashTags, ?TWEET_HASHTAG_LIM)
                                            ),
                                    send_message(
                                        ClientPid,
                                        {send_tweet, TweetWithMentionsAndHashtags}
                                    )
                                end,
                                lists:seq(1, trunc(NumSubscribers / ?SUB2TWEETS))
                            ),
                            simulation_loop(
                                Start,
                                NumClients,
                                StartTime,
                                NumRequests,
                                CurrentNumRequests + trunc(NumSubscribers / ?SUB2TWEETS),
                                LiveUsersList,
                                DisconnectedUsersList,
                                Tweets,
                                HashTags
                            )
                    end;
                2 ->
                    % Query and Retweet
                    if
                        LiveUsersList == [] ->
                            simulation_loop(
                                Start,
                                NumClients,
                                StartTime,
                                NumRequests,
                                CurrentNumRequests,
                                LiveUsersList,
                                DisconnectedUsersList,
                                Tweets,
                                HashTags
                            );
                        true ->
                            {Username, ClientPid, NumSubscribers} = lists:nth(
                                rand:uniform(length(LiveUsersList)), LiveUsersList
                            ),
                            lists:foreach(
                                fun(_) ->
                                    io:format("[Simulator~w] Invoking query on ~p.\n", [
                                        Start, Username
                                    ]),
                                    send_message(
                                        ClientPid,
                                        {query,
                                            sample_mentions(
                                                Start, NumClients, ?QUERY_MENTIONS_LIM, false
                                            ),
                                            sample_hashtags(HashTags, ?QUERY_HASHTAGS_LIM, false),
                                            sample_mentions(
                                                Start, NumClients, ?QUERY_MENTIONS_LIM, false
                                            )}
                                    ),

                                    receive
                                        {Username, QueriedTweets} ->
                                            if
                                                QueriedTweets == [] ->
                                                    simulation_loop(
                                                        Start,
                                                        NumClients,
                                                        StartTime,
                                                        NumRequests,
                                                        CurrentNumRequests,
                                                        LiveUsersList,
                                                        DisconnectedUsersList,
                                                        Tweets,
                                                        HashTags
                                                    );
                                                true ->
                                                    DoRetweet = rand:uniform(2) - 1,
                                                    case DoRetweet of
                                                        1 ->
                                                            io:format(
                                                                "[Simulator~w] Invoking retweet on ~p.\n",
                                                                [
                                                                    Start, Username
                                                                ]
                                                            ),
                                                            send_message(
                                                                ClientPid,
                                                                {retweet,
                                                                    element(
                                                                        1,
                                                                        lists:nth(
                                                                            rand:uniform(
                                                                                length(
                                                                                    QueriedTweets
                                                                                )
                                                                            ),
                                                                            QueriedTweets
                                                                        )
                                                                    )}
                                                            ),
                                                            simulation_loop(
                                                                Start,
                                                                NumClients,
                                                                StartTime,
                                                                NumRequests,
                                                                CurrentNumRequests + 2,
                                                                LiveUsersList,
                                                                DisconnectedUsersList,
                                                                Tweets,
                                                                HashTags
                                                            );
                                                        0 ->
                                                            simulation_loop(
                                                                Start,
                                                                NumClients,
                                                                StartTime,
                                                                NumRequests,
                                                                CurrentNumRequests + 1,
                                                                LiveUsersList,
                                                                DisconnectedUsersList,
                                                                Tweets,
                                                                HashTags
                                                            )
                                                    end
                                            end
                                    end
                                end,
                                lists:seq(1, trunc(NumSubscribers / ?SUB2TWEETS))
                            )
                    end;
                3 ->
                    % Disconnect
                    if
                        LiveUsersList == [] ->
                            simulation_loop(
                                Start,
                                NumClients,
                                StartTime,
                                NumRequests,
                                CurrentNumRequests,
                                LiveUsersList,
                                DisconnectedUsersList,
                                Tweets,
                                HashTags
                            );
                        true ->
                            {Username, ClientPid, Subscribers} = lists:nth(
                                rand:uniform(length(LiveUsersList)), LiveUsersList
                            ),
                            io:format("[Simulator~w] Invoking disconnect on ~p.\n", [
                                Start, Username
                            ]),
                            send_message(ClientPid, {send_disconnect}),

                            simulation_loop(
                                Start,
                                NumClients,
                                StartTime,
                                NumRequests,
                                CurrentNumRequests + 1,
                                lists:keydelete(Username, 1, LiveUsersList),
                                DisconnectedUsersList ++ [{Username, ClientPid, Subscribers}],
                                Tweets,
                                HashTags
                            )
                    end;
                4 ->
                    % Connect
                    if
                        DisconnectedUsersList == [] ->
                            simulation_loop(
                                Start,
                                NumClients,
                                StartTime,
                                NumRequests,
                                CurrentNumRequests,
                                LiveUsersList,
                                DisconnectedUsersList,
                                Tweets,
                                HashTags
                            );
                        true ->
                            {Username, ClientPid, Subscribers} = lists:nth(
                                rand:uniform(length(DisconnectedUsersList)), DisconnectedUsersList
                            ),
                            io:format("[Simulator~w] Invoking connect on ~p.\n", [Start, Username]),
                            send_message(ClientPid, {send_connect}),

                            simulation_loop(
                                Start,
                                NumClients,
                                StartTime,
                                NumRequests,
                                CurrentNumRequests + 1,
                                LiveUsersList ++ [{Username, ClientPid, Subscribers}],
                                lists:keydelete(Username, 1, DisconnectedUsersList),
                                Tweets,
                                HashTags
                            )
                    end
            end
    end.

sample_mentions(Start, NumClients, NumSamples) ->
    sample_mentions(Start, NumClients, NumSamples, true).

sample_mentions(Start, NumClients, NumSamples, Exact) ->
    if
        Exact == true ->
            lists:uniq(
                lists:map(
                    fun(_) -> get_username(rand:uniform(Start + NumClients - 1)) end,
                    lists:seq(1, NumSamples)
                )
            );
        true ->
            lists:uniq(
                lists:map(
                    fun(_) -> get_username(rand:uniform(Start + NumClients - 1)) end,
                    lists:seq(1, rand:uniform(NumSamples) - 1)
                )
            )
    end.

sample_hashtags(HashTags, NumSamples) ->
    sample_hashtags(HashTags, NumSamples, true).

sample_hashtags(HashTags, NumSamples, Exact) ->
    if
        Exact == true ->
            lists:uniq(
                lists:map(
                    fun(_) -> lists:nth(rand:uniform(length(HashTags)), HashTags) end,
                    lists:seq(1, NumSamples)
                )
            );
        true ->
            lists:uniq(
                lists:map(
                    fun(_) -> lists:nth(rand:uniform(length(HashTags)), HashTags) end,
                    lists:seq(1, rand:uniform(NumSamples) - 1)
                )
            )
    end.

get_command_to_invoke() ->
    rand:uniform(4).

get_username(UserNum) ->
    lists:concat(["User", UserNum]).

get_subscribers(Start, NumClients, ForUser) ->
    Val = get_zipf_value(?ZIPF_POWER, NumClients),
    Subscribers = lists:map(
        fun(_) -> get_username(rand:uniform(Start + NumClients - 1)) end,
        lists:seq(1, Val)
    ),
    lists:uniq(lists:delete(ForUser, Subscribers)).

get_zipf_value(Power, N) ->
    C = calculate_normalization_constant(Power, N, 1, 0),
    Z = rand:uniform(),
    map_z_to_value(Power, N, 1, 0, Z, C).

map_z_to_value(Power, N, Iter, SumProbability, Z, C) ->
    if
        Iter > N ->
            ok;
        true ->
            NewSumProbability = SumProbability + C / math:pow(Iter, Power),
            if
                NewSumProbability >= Z ->
                    Iter;
                true ->
                    map_z_to_value(Power, N, Iter + 1, NewSumProbability, Z, C)
            end
    end.

calculate_normalization_constant(Power, N, Iter, C) ->
    if
        Iter > N ->
            1 / C;
        true ->
            calculate_normalization_constant(Power, N, Iter + 1, C + (1 / math:pow(Iter, Power)))
    end.

read_file(Filename) ->
    {ok, Device} = file:open(Filename, [read]),
    try
        get_all_lines(Device)
    after
        file:close(Device)
    end.

get_all_lines(Device) ->
    case io:get_line(Device, "") of
        eof -> [];
        Line -> [lists:delete($\n, Line)] ++ get_all_lines(Device)
    end.

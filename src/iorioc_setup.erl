-module(iorioc_setup).
-export([setup_access/1]).

-include("include/iorio.hrl").
-include_lib("permiso/include/permiso.hrl").

setup_access(Opts) ->
    % TODO: check here that secret is a valid one
    {ok, ApiSecret} = env(Opts, auth_secret),

    AdminUsername = env(Opts, admin_username, "admin"),
    {ok, AdminPassword} = env(Opts, admin_password),

    AnonUsername = env(Opts, anon_username, "anonymous"),
    % default password since login in as anonymous is not that useful
    AnonPassword = env(Opts, anon_password, <<"secret">>),

    AuthMod = env(Opts, auth_mod, permiso_rcore),
    AuthModOpts0 = env(Opts, auth_mod_opts, []),

    OnUserCreated = fun (Mod, State, #user{username=Username}) ->
                            ioriol_access:maybe_grant_bucket_ownership(Mod, State, Username),
                            Mod:user_join(State, Username, ?USER_GROUP)
                    end,
    AuthModOpts = [{user_created_cb, OnUserCreated}|AuthModOpts0],
    {ok, AccessLogic} = ioriol_access:new([{auth_mod, AuthMod},
                                           {auth_mod_opts, AuthModOpts},
                                           {secret, ApiSecret}]),

    CreateGroupsResult = create_groups(AccessLogic),
    lager:info("create groups ~p", [CreateGroupsResult]),

    GrantAdminUsers = fun (Username) ->
                              Res = ioriol_access:grant(AccessLogic, AdminUsername,
                                                        ?PERM_MAGIC_BUCKET, any,
                                                        ?PERM_ADMIN_USERS),
                              lager:info("assign admin users to ~p: ~p",
                                         [Username, Res]),
                              ioriol_access:maybe_grant_bucket_ownership(AccessLogic, AdminUsername)
                      end,
    create_user(AccessLogic, AdminUsername, AdminPassword, [?ADMIN_GROUP],
                GrantAdminUsers),
    create_user(AccessLogic, AnonUsername, AnonPassword, [],
                fun (_) -> ok end),

    setup_initial_permissions(AccessLogic, AdminUsername),
    {ok, AccessLogic}.

create_user(Access, Username, Password, Groups, OnUserCreated) ->
    case ioriol_access:create_user(Access, Username, Password, Groups) of
        ok ->
            lager:info("~p user created", [Username]),
            OnUserCreated(Username);
        {error, duplicate}  ->
            lager:info("~p user exists", [Username]);
        OtherError ->
            lager:error("creating ~p user ~p", [Username, OtherError])
    end.

setup_initial_permissions(AccessLogic, AdminUsername) ->
    PublicReadBucket = <<"public">>,
    R1 = ioriol_access:grant(AccessLogic, <<"*">>, PublicReadBucket, any,
                             "iorio.get"),
    lager:info("set read permissions to ~s to all: ~p",
               [PublicReadBucket, R1]),
    R2 = ioriol_access:grant(AccessLogic, list_to_binary(AdminUsername),
                             PublicReadBucket, any, "iorio.put"),
    lager:info("set write permissions to ~s to ~p: ~p",
               [PublicReadBucket, AdminUsername, R2]).

create_group(AccessLogic, Name) ->
    lager:info("creating group ~p", [Name]),
    case ioriol_access:add_group(AccessLogic, Name) of
        ok ->
            lager:info("~p group created", [Name]);
        {error, duplicate} ->
            lager:info("~p group exists", [Name]);
        Other ->
            lager:warning("unknown response in group ~p creation '~p'",
                       [Name, Other])
    end.

create_groups(AccessLogic) ->
    lists:foreach(fun (Group) -> create_group(AccessLogic, Group) end,
                  ?ALL_GROUPS),
    % TODO: move this to permiso
    UserGroup = ?USER_GROUP,
    AdminGroup = list_to_binary(?ADMIN_GROUP),
    ioriol_access:group_inherit(AccessLogic, AdminGroup, [UserGroup]).

env(Opts, Key) ->
    case proplists:lookup(Key, Opts) of
        {Key, Value} -> {ok, Value};
        undefined -> undefined
    end.

env(Opts, Key, Def) ->
    proplists:get_value(Key, Opts, Def).


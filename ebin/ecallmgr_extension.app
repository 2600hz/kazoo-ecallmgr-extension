{application, 'ecallmgr_extension'
,[
  {description, "Extension to ecallmgr"}
 ,{vsn, "4.0.0"}
 ,{modules, [ecallmgr_ext_app, ecallmgr_ext_sup, ecallmgr_fs_amqp, ecallmgr_fs_amqp_config, ecallmgr_fs_amqp_handler, ecallmgr_fs_amqp_listener, ecallmgr_fs_event_amqp_sup, ecallmgr_fs_extension_node_sup, ecallmgr_fs_metaflow, kapi_freeswitch, metaflow_action, metaflow_bind, metaflow_control, metaflow_flow, metaflow_fsm, metaflow_fsm_sup, metaflow_route, metaflow_sup]}
 ,{registered, [ ]}
 ,{applications, [ kazoo_apps, ecallmgr]}
 ,{mod, {ecallmgr_ext_app, []}}
 ,{env, [{amqp, [{producers, [{channel, [{events, ["CHANNEL_CREATE"
                                                  ,"CHANNEL_ANSWER"
                                                  ,"CHANNEL_DESTROY"
                                                  ]}
                                        ,{params, [{"exchange-type", "topic"}
                                                  ,{"exchange-name", "freeswitch"}
                                                  ,{"exchange-durable", false}
                                                  ,{"content-type", "application/json"}
                                                  ,{"delivery-timestamp", false}
                                                  ,{"delivery-mode", 0}
                                                  ,{"circuit_breaker_ms", 10000}
                                                  ,{"reconnect_interval_ms", 1}
                                                  ,{"send_queue_size", 25000}
                                                  ,{"enable_fallback_format_fields", 1}
                                                  ,{"format_fields", "#FreeSWITCH,#channel,FreeSWITCH-Hostname,Event-Subclass|Event-Name,Unique-ID|Core-UUID"}
                                                  ]}
                                        ]
                              }
                             ,{presence, [{events, ["CUSTOM:KZ_PRESENCE_DIALOG"
                                                   ]}
                                         ,{params, [{"exchange-type", "topic"}
                                                   ,{"exchange-name", "freeswitch"}
                                                   ,{"exchange-durable", false}
                                                   ,{"content-type", "application/json"}
                                                   ,{"delivery-timestamp", false}
                                                   ,{"delivery-mode", 0}
                                                   ,{"circuit_breaker_ms", 10000}
                                                   ,{"reconnect_interval_ms", 1}
                                                   ,{"send_queue_size", 25000}
                                                   ,{"enable_fallback_format_fields", 1}
                                                   ,{"format_fields", "#FreeSWITCH,#presence,FreeSWITCH-Hostname,Event-Subclass|Event-Name,Unique-ID|Core-UUID"}
                                                   ]}
                                         ]
                              }
                             ,{recordings, [{events, ["RECORD_START"
                                                     ,"RECORD_STOP"
                                                     ]}
                                           ,{params, [{"exchange-type", "topic"}
                                                     ,{"exchange-name", "freeswitch"}
                                                     ,{"exchange-durable", false}
                                                     ,{"content-type", "application/json"}
                                                     ,{"delivery-timestamp", false}
                                                     ,{"delivery-mode", 0}
                                                     ,{"circuit_breaker_ms", 10000}
                                                     ,{"reconnect_interval_ms", 1}
                                                     ,{"send_queue_size", 25000}
                                                     ,{"enable_fallback_format_fields", 1}
                                                     ,{"format_fields", "#FreeSWITCH,#recordings,FreeSWITCH-Hostname,Event-Subclass|Event-Name,Unique-ID|Core-UUID"}
                                                     ]}
                                           ]
                              }
                              %%
                              %% freeswitch mod_amqp does not handle custom events for now
                              %%
                             ,{conference, [{events, ["CUSTOM:conference::maintenance"
                                                     ]}
                                           ,{params, [{"exchange-type", "topic"}
                                                     ,{"exchange-name", "freeswitch"}
                                                     ,{"exchange-durable", false}
                                                     ,{"content-type", "application/json"}
                                                     ,{"delivery-timestamp", false}
                                                     ,{"delivery-mode", 0}
                                                     ,{"circuit_breaker_ms", 10000}
                                                     ,{"reconnect_interval_ms", 1}
                                                     ,{"send_queue_size", 25000}
                                                     ,{"enable_fallback_format_fields", 1}
                                                     ,{"format_fields", "#FreeSWITCH,#conference,FreeSWITCH-Hostname,Event-Subclass|Event-Name,Conference-Name|Conference-Unique-ID|Unique-ID|Core-UUID"}
                                                     ]}
                                           ]
                              }
                             ,{heartbeat, [{events, ["HEARTBEAT"
                                                    ]}
                                          ,{queue_name, <<>>}
                                          ,{queue_options, []}
                                          ,{consume_options, []}
                                          ,{params, [{"exchange-type", "topic"}
                                                    ,{"exchange-name", "freeswitch"}
                                                    ,{"exchange-durable", false}
                                                    ,{"content-type", "application/json"}
                                                    ,{"delivery-timestamp", false}
                                                    ,{"delivery-mode", 0}
                                                    ,{"circuit_breaker_ms", 10000}
                                                    ,{"reconnect_interval_ms", 1}
                                                    ,{"send_queue_size", 25000}
                                                    ,{"enable_fallback_format_fields", 1}
                                                    ,{"format_fields", "#FreeSWITCH,#heartbeat,FreeSWITCH-Hostname,Event-Subclass|Event-Name,Conference-Name|Conference-Unique-ID|Unique-ID|Core-UUID"}
                                                    ]}
                                          ]
                              }
                             ]
                 }]
         }
        ,{node_modules, ["event_amqp_sup"
                        ,"metaflow"
                        ]}
        ,{schemas_to_priv, true}
        ]}
 ]}.

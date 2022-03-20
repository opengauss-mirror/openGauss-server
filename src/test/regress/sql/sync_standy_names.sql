show synchronous_standby_names;

-- single gram
alter system set synchronous_standby_names = '*';                                                          -- suc
alter system set synchronous_standby_names = 'd1, *';                                                      -- err, semantic ambiguity
alter system set synchronous_standby_names = '*, *';                                                       -- err, semantic ambiguity
alter system set synchronous_standby_names = 'd1';                                                         -- suc
alter system set synchronous_standby_names = 'd1, d2, d3, d4';                                             -- suc
alter system set synchronous_standby_names = 'd1, d2, d1, d4';                                             -- err, duplicate name
alter system set synchronous_standby_names = '2 (*)';                                                      -- suc
alter system set synchronous_standby_names = '2 (d1, *)';                                                  -- err, semantic ambiguity
alter system set synchronous_standby_names = '2 (*, *)';                                                   -- err, semantic ambiguity
alter system set synchronous_standby_names = '2 (d1, d2, d3, d4)';                                         -- suc
alter system set synchronous_standby_names = '2 (d1, d2, d1, d4)';                                         -- err, duplicate name
alter system set synchronous_standby_names = '5 (d1, d2, d3, d4)';                                         -- err, requriement more than have
alter system set synchronous_standby_names = '0 (d1, d2, d3, d4)';                                         -- suc
alter system set synchronous_standby_names = '-1 (d1, d2, d3, d4)';                                        -- err, parser err
alter system set synchronous_standby_names = 'first 2 (*)';                                                -- suc
alter system set synchronous_standby_names = 'first 2 (d1, *)';                                            -- err, semantic ambiguity
alter system set synchronous_standby_names = 'first 2 (*, *)';                                             -- err, semantic ambiguity
alter system set synchronous_standby_names = 'first 2 (d1, d2, d3, d4)';                                   -- suc
alter system set synchronous_standby_names = 'first 2 (d1, d2, d1, d4)';                                   -- err, duplicate name
alter system set synchronous_standby_names = 'first 5 (d1, d2, d3, d4)';                                   -- err, requriement more than have
alter system set synchronous_standby_names = 'first 0 (d1, d2, d3, d4)';                                   -- suc
alter system set synchronous_standby_names = 'first -1 (d1, d2, d3, d4)';                                  -- err, parser err
alter system set synchronous_standby_names = 'any 2 (*)';                                                  -- suc
alter system set synchronous_standby_names = 'any 2 (d1, *';                                               -- err, semantic ambiguity
alter system set synchronous_standby_names = 'any 2 (*, *)';                                               -- err, semantic ambiguity
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4)';                                     -- suc
alter system set synchronous_standby_names = 'any 2 (d1, d2, d1, d4)';                                     -- err, duplicate name
alter system set synchronous_standby_names = 'any 5 (d1, d2, d3, d4)';                                     -- err, requriement more than have
alter system set synchronous_standby_names = 'any 0 (d1, d2, d3, d4)';                                     -- suc
alter system set synchronous_standby_names = 'any -1 (d1, d2, d3, d4)';                                    -- err, parser err

-- combinate gram
alter system set synchronous_standby_names = 'd1, d2, d3, 1 (d5, d6)';                                     -- err, parser err
alter system set synchronous_standby_names = '1 (d1, d2, d3), 1 (d5, d6)';                                 -- err, parser err
alter system set synchronous_standby_names = '*, 1 (d5, d6)';                                              -- err, parser err
alter system set synchronous_standby_names = '1 (d1, d2, d3), 1 (*)';                                      -- err, parser err
alter system set synchronous_standby_names = '1 (d1, d2, d3), 1 (*)';                                      -- err, parser err
alter system set synchronous_standby_names = 'any 2 (*), any 2 (d1, d2, d3, d4), any 2 (d5, d6, d7)';      -- err, semantic ambiguity
alter system set synchronous_standby_names = 'any 2 (d, dd), any 2 (d1, *, d3, d4), any 2 (d5, d6, d7)';   -- err, semantic ambiguity
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), d5, d6, d7';                         -- err, parser err
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), 2 (d5, d1, d7)';                     -- err, parser err
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), *';                                  -- err, parser err
alter system set synchronous_standby_names = '2 (d1, d2, d3, d4), first 2 (d5, d6, d7)';                   -- err, parser err
alter system set synchronous_standby_names = '2 (d1, d2, d3, d4), any 4 (d5, d6, d7)';                     -- err, parser err
alter system set synchronous_standby_names = '*, any 0 (d5, d6, d7)';                                      -- err, parser err
alter system set synchronous_standby_names = 'd1, d2, d3, d4, any 2 (d5, d6, d7)';                         -- err, parser err
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), any 2 (d5, d6, d7)';                 -- suc
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), any 2 (d5, d1, d7)';                 -- err, duplicate name
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), any 2 (d5, d6, d6)';                 -- err, duplicate name
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), first 2 (d5, d6, d7)';               -- err, not support first
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), any 4 (d5, d6, d7)';                 -- err, requriement more than have
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), any 0 (d5, d6, d7)';                 -- suc
alter system set synchronous_standby_names = 'first 2 (d1, d2, d3, d4), first 2 (d5, d6, d7)';             -- err, not support first
alter system set synchronous_standby_names = 'any 2 (d1, d2, d3, d4), , any 0 (d5, d6, d7)';               -- err, parser err

-- recover
alter system set synchronous_standby_names = '*';
select pg_sleep(3);
show synchronous_standby_names;
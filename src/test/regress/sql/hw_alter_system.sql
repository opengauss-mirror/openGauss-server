ALTER SYSTEM KILL SESSION '1, 0' IMMEDIATE;
ALTER SYSTEM DISCONNECT SESSION '1, 0' IMMEDIATE;
ALTER SYSTEM kll SESSION '1, 0' IMMEDIATE;
ALTER SYSTEM KILL DISCONNECT SESSION '1, 0' IMMEDIATE;

ALTER SYSTEM KILL SESSION '' IMMEDIATE;
ALTER SYSTEM KILL SESSION '1' IMMEDIATE;
ALTER SYSTEM KILL SESSION 's' IMMEDIATE;
ALTER SYSTEM KILL SESSION '1 1' IMMEDIATE;
ALTER SYSTEM KILL SESSION '1,' IMMEDIATE;--
ALTER SYSTEM KILL SESSION ',1' IMMEDIATE;--
ALTER SYSTEM KILL SESSION '1,1,1' IMMEDIATE;
ALTER SYSTEM KILL SESSION '-1, 1' IMMEDIATE;

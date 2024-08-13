-- Total Variables
SELECT name FROM system.databases; -- ${database}
SELECT name FROM system.tables WHERE database IN (${database}); -- ${table}

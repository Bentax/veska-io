-- Total Variables
SELECT name FROM system.databases; -- ${database}
SELECT name FROM system.tables WHERE database IN (${database}); -- ${table}

-- Version 24.5.1.22926
SELECT version();

-- Server uptime 1.41 hour
SELECT uptime() as uptime

-- Total rows 11651
SELECT  sum(total_rows) as "Total rows" FROM system.tables WHERE database IN (${database}) AND name IN (${table})

-- Total columns 1400
SELECT count() as "Total columns" FROM system.columns WHERE database IN (${database}) AND table IN (${table})

-- Disk usage
SELECT
    name as Name,
    path as Path,
    formatReadableSize(free_space) as Free,
    formatReadableSize(total_space) as Total,
    1 - free_space/total_space as Used
FROM system.disks

-- Top tables by rows
SELECT concatAssumeInjective(table.database, '.', name) as name,
       table_stats.total_rows as total_rows
FROM system.tables table
         LEFT JOIN ( SELECT table,
       database,
       sum(rows)                  as total_rows
FROM system.parts
WHERE table IN (${table}) AND active AND database IN (${database}) 
GROUP BY table, database
 ) AS table_stats ON table.name = table_stats.table AND table.database = table_stats.database
ORDER BY total_rows DESC
LIMIT 10

-- 

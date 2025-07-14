WITH strategy_data (parentid, segmentid, strategyid, strategyname, position, path)
AS
(
-- Anchor member definition
    SELECT 
		segments_rep.parentid,
		segments_rep.id AS segmentid,
		segments_rep.id AS strategyid,
		segments_rep.name AS strategyname, 
		REPLACE(STR(ISNULL(segments_rep.priority,0),3), ' ', '0') AS position, 
		CONVERT(varchar(1000), '\' + segments_rep.name) AS path
    FROM 
		segments_rep
    WHERE 
		segments_rep.parentid IS NULL
UNION ALL
-- Recursive member definition
    SELECT
		segments_rep.parentid, 
		segments_rep.id AS segmentid, 
		strategy_data.strategyid AS strategyid,
		strategy_data.strategyname AS strategyname,
		CASE 
		  	WHEN DataLength(strategy_data.position) > 0
		 	THEN CONVERT(varchar(1000),strategy_data.position + REPLACE(STR(ISNULL(segments_rep.priority,0),3), ' ', '0'))
		  	ELSE REPLACE(STR(ISNULL(segments_rep.priority,0),3), ' ', '0') 
		END AS position, 
		CASE
			WHEN DataLength(strategy_data.path) > 0
		      THEN CONVERT(varchar(1000),strategy_data.path + '\'+ segments_rep.name) 
			  ELSE CONVERT(varchar(1000), segments_rep.name) 
	    END AS path
    FROM 
		segments_rep, 
		strategy_data
    WHERE 
		segments_rep.parentid = strategy_data.segmentid
)
SELECT
	SEGMENTID,
	STRATEGYID,
	STRATEGYNAME,
	position + REPLACE(STR('',30-LEN(position)),' ','0') AS POSITION,
	PATH
FROM 
	strategy_data 
;

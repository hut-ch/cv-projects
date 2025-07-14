MERGE INTO dim_release
USING (
	SELECT 
		release_id 
		, release_number 
		, release_name 
		, release_developer_notes 
		, release_notes 
		--......
	FROM stage_release_data
	EXCEPT
	SELECT 
		release_id 
		, release_number  
		, release_name  
		, release_developer_notes  
		, release_notes  
		--......
	FROM dim_release
) AS changes 
ON (dim_release.release_id = changes.release_id)
WHEN MATCHED THEN 
UPDATE 
	SET     
	release_number =  changes.release_number
	, release_name =  changes.release_name
	, release_developer_notes =  changes.release_developer_notes
	, release_notes =  changes.release_notes
	--......
	, update_time = getdate()
WHEN NOT MATCHED THEN 
	INSERT (       
		release_id_kunagi
		, release_number
		, release_name
		, release_developer_notes
		, release_notes
		--......
		, dss_update_time
	)
	VALUES (      
		release_id_kunagi
		, release_number
		, release_name
		, release_developer_notes
		, release_notes
		--......
		, getdate()
  );

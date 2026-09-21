The dataset in the landing_zone folder is committed to the repo as a zip file to save space.
To use it, simply extract archive.zip in place. The etl_pipeline expects the data to be in landing_zone/archive.
The gitignore already includes the archive folder, so the unzipped dataset shouldn't be committed. 

Once again, ensure your .env file has MONGODB_URI. 
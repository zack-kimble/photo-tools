##Pseudocode
```
Load configs
get all leaf directories for each parent source directory in configs
create tasks in multiproces queue for each leaf directory
for each task
    get all files in leaf directory
    for each file
        extract exif data using exiftool or Pillow
        attempt to normalize key exif data (datetime, rating, label color)
        create a photo_source_file object and write to database
        create a photo object if it doesn't exist and write to database (ust ts_id right now)
for each photo object in db
    retrieve all source file objects
    determine reference source file based on preference order provided in config and add to photo object
    merge exif data from source file objects and add to photo object
    merge label_color and rating from source file objects and add to photo object
    update photo object in db

    
   
        
        

```

// This class keeps track of the number of times a file has been uploaded by a user on a particular day
case class Uploads(fileId: Long, userId: Long, ds: String, numberOfUploads: Int)
// This class keeps track of the number of times a file has been downloaded  by a user on a particular day
case class Downloads(fileId: Long, userId: Long, ds: String, numberOfDownloads: Int)
// This class keeps track of the number of times a file has been accessed by a user on a particular day
case class Access(fileId: Long, userId: Long, ds: String, numberofUploads: Int, numberOfDownloads: Int)

// contains download and upload events for the past 4 days. 
val uploads = sc.textFile("file:///Users/srao/Box Sync/IBU_revive/airpal/uploads.csv")
val downloads = sc.textFile("file:///Users/srao/Box Sync/IBU_revive/airpal/downloads.csv")

// split by , take out ending and beginning quotes " " and then the first column contains the column names, take those out
val cleaned_uploads = uploads.map(s => s.split(",").map(e=> e.replaceAll("^\"|\"$",""))).filter(e => e(0)!="file_id")
val cleaned_downloads = downloads.map(s => s.split(",").map(e=> e.replaceAll("^\"|\"$",""))).filter(e => e(0)!="file_id")

// Create the data frames and register than as temp tables
val DownloadsDF = cleaned_downloads.map(e=> Downloads(e(0).toLong,e(1).toLong,e(2),e(3).toInt)).toDF()
val UploadsDF = cleaned_uploads.map(e=> Uploads(e(0).toLong,e(1).toLong,e(3),e(2).toInt)).toDF()
UploadsDF.registerTempTable("uploads")
DownloadsDF.registerTempTable("downloads")

// join the tables. Since this is an outer join, one of the first 4 or last 4 columns might be null
val joined = sqlContext.sql(" SELECT * FROM uploads u FULL OUTER JOIN downloads d ON u.userId == d.userId and u.fileId == d.fileId and u.ds == d.ds")

// weed out the nulls and create the resulting Row
// if the downloads data is empty for this (userid, fileid, ds) combo, count downloads as 0
// if the uploads data is empty for this (userid, fileid, ds) combo, count uploads as 0
val joined_cleaned  = joined.map{ e => {
if (e.isNullAt(4)) Row(e.getLong(0), e.getLong(1), e.getString(2), e.getInt(3), 0) 
else {if (e.isNullAt(0)) Row(e.getLong(4), e.getLong(5), e.getString(6), 0, e.getInt(7))
else Row(e.getLong(0), e.getLong(1), e.getString(2), e.getInt(3), e.getInt(7))}}}
// create the access DF
val access = joined_cleaned.map(e=> Access(e.getLong(0),e.getLong(1),e.getString(2), e.getInt(3),e.getInt(4))).toDF()
// register this as the access table
access.registerTempTable("access")

// Now total the number of times the file has been uploaded and downloaded in the past 4  days
val file_scores = sqlContext.sql("SELECT fileId, userId, SUM(numberOfUploads)+ SUM(numberOfDownloads) as freq FROM access GROUP BY fileId, userId ORDER BY freq DESC")

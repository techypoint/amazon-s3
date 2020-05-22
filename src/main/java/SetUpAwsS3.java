import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class SetUpAwsS3 {

  private static Logger LOG = Logger.getLogger("logger");

  public static void main(String[] args) {

    // now create BasicAWSCredentials object by using access key and secret key
    BasicAWSCredentials basicAWSCredentials=new BasicAWSCredentials("AKIAIOKOL7QLI4OL6NAA",
        "nszcxdwDuLwgNGrYSDRC0s48XfXcuzR6pmsGIGkb");

    // get AmazonS3 Client by passing basicAWSCredentials and specify region which is closest to you
    // or let it pick default region
    AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
        .withRegion(Regions.AP_SOUTH_1)
        .build();
    String bucketNameToCreate="bucketnametocreate";

//    createBucket(amazonS3,bucketNameToCreate);
//    listBuckets(amazonS3);
//    uploadObjectInABucket(amazonS3,bucketNameToCreate);
//    listObjects(amazonS3,bucketNameToCreate);
    listObjectsWithLimit(amazonS3,bucketNameToCreate);
  }

  public static void createBucket(AmazonS3 amazonS3,String bucketNameToCreate){
    if(!amazonS3.doesBucketExistV2(bucketNameToCreate)){
      Bucket bucket=amazonS3.createBucket(new CreateBucketRequest(bucketNameToCreate));
    }else{
      LOG.info("Bucket name already exists");
    }
  }
  public static void listBuckets(AmazonS3 amazonS3){
    List<Bucket> bucketList=amazonS3.listBuckets();
    for(Bucket bucket : bucketList) {
      LOG.info("bucket name - "+bucket.getName());
    }
  }

  // it is important to empty bucket before deleting it
  public static void deleteBuckets(AmazonS3 amazonS3,String bucketName){

    // delete all objects in a bucket
    ObjectListing listObjects = amazonS3.listObjects(bucketName);
    List<S3ObjectSummary> s3ObjectSummaryList=listObjects.getObjectSummaries();
    while (true){
      for(S3ObjectSummary s3ObjectSummary:s3ObjectSummaryList){
        amazonS3.deleteObject(bucketName,s3ObjectSummary.getKey());
      }
      if(listObjects.isTruncated()){
        // listNextBatchOfObjects - this will next batch of objects
        listObjects=amazonS3.listNextBatchOfObjects(listObjects);
      }else{
        break;
      }
    }

    // delete all version objects in a buckets
    VersionListing versionList = amazonS3.listVersions(new ListVersionsRequest().withBucketName(bucketName));
    List<S3VersionSummary> versionSummaryList=versionList.getVersionSummaries();
    while (true){
      for(S3VersionSummary s3VersionSummary:versionSummaryList){
        amazonS3.deleteVersion(bucketName,s3VersionSummary.getKey(),s3VersionSummary.getVersionId());
      }
      if(versionList.isTruncated()){
        // listNextBatchOfObjects - this will next batch of objects
        versionList=amazonS3.listNextBatchOfVersions(versionList);
      }else{
        break;
      }
    }
    // finally delete bucket
    amazonS3.deleteBucket(bucketName);
  }

  public static void listObjects(AmazonS3 amazonS3,String bucketName){
    // list all objects in a buckets
    ObjectListing objectListing=amazonS3.listObjects(bucketName);
    for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
      LOG.info("key-"+os.getKey());
      LOG.info("Size -"+os.getSize());
    }
  }

  // list x no. of objects of a buckets at a time
  // and get more object keys if require
  public static void listObjectsWithLimit(AmazonS3 amazonS3,String bucketName){
    ListObjectsV2Request listObjectsV2Request=new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(5);
    ListObjectsV2Result listObjectsV2Result=amazonS3.listObjectsV2(listObjectsV2Request);
    while (true){
      List<S3ObjectSummary> s3ObjectSummaryList=listObjectsV2Result.getObjectSummaries();
      for(S3ObjectSummary s3ObjectSummary:s3ObjectSummaryList){
        // you can other values of s3ObjectSummary
        LOG.info("key- "+s3ObjectSummary.getKey());
      }
      if(listObjectsV2Result.isTruncated()){
        String token=listObjectsV2Result.getNextContinuationToken();
        listObjectsV2Request.setContinuationToken(token);
      }else{
        break;
      }
    }
  }

  public static void uploadObjectInABucket(AmazonS3 amazonS3,String bucketName){

    // code to upload a text as a object
    String objectKey1="object key 1";
    String objectValue="Upload a Text String";
    amazonS3.putObject(bucketName,objectKey1,objectValue);

    // code to upload a file as a object
    String objectKey2="object key 2";
    String pathToFile="Path of File";
    PutObjectRequest putObjectRequest=new PutObjectRequest(bucketName,objectKey2,
        new File(pathToFile));
    ObjectMetadata objectMetadata=new ObjectMetadata();
    objectMetadata.setContentType("application/pdf");
    // use can set other meta data as a key value pair in ObjectMetadata
    objectMetadata.addUserMetadata("metaDataKey","metaDataValue");
    putObjectRequest.setMetadata(objectMetadata);
    amazonS3.putObject(putObjectRequest);
  }

  public static void deleteObject(AmazonS3 amazonS3,String bucketName){
    String objectKey="object key";
    amazonS3.deleteObject(new DeleteObjectRequest(bucketName,objectKey));
  }

  // copy object in bucketName1 having key - objectKey1 to bucketName2 having key - objectKey2
  public static void copyObjectFromOneKeyToAnother(AmazonS3 amazonS3){
    String bucketName1="bucketName1"; // source bucket name
    String bucketName2="bucketName2"; // destination bucket name
    String objectKey1="objectKey1";   // source object key
    String objectKey2="objectKey2";   // destination object key
    amazonS3.copyObject(new CopyObjectRequest(bucketName1,objectKey1,bucketName2,objectKey2));
  }

  public static void generatePreSignedURL(AmazonS3 amazonS3,String bucketName,String objectKey){

    Calendar calendar= Calendar.getInstance();
    calendar.add(Calendar.DAY_OF_MONTH,5); // expiration time - 5 days

    GeneratePresignedUrlRequest generatePresignedUrlRequest =
        new GeneratePresignedUrlRequest(bucketName,objectKey)
            .withMethod(HttpMethod.GET)
            .withExpiration(calendar.getTime());

    URL url = amazonS3.generatePresignedUrl(generatePresignedUrlRequest);
    LOG.info("URL- " + url.toString());
  }

  // it creates URL for public objects
  public static void generatePublicURL(AmazonS3 amazonS3,String bucketName,String objectKey){
    URL url=amazonS3.getUrl(bucketName,objectKey);
    LOG.info("URL- " + url.toString());
  }

  public static void createLifeCycleConfiguration(AmazonS3 amazonS3,String bucketName){
    // rule1 move objects to Glacier Storage class after 60 days and
    // delete objects after 120 days
    BucketLifecycleConfiguration.Rule rule=new Rule()
        .withId("rule id 1")
        .addTransition(new Transition().withDays(60).withStorageClass(StorageClass.Glacier))
        .withExpirationInDays(120)
        .withStatus(BucketLifecycleConfiguration.ENABLED);

    // apply this rules to the objects which gets filtered through prefix
    // LifecyclePrefixPredicate - to filter objects through prefix to report
    BucketLifecycleConfiguration.Rule rule2=new Rule()
        .withId("rule id 1")
        .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("heavyReports/")))
        .addTransition(new Transition().withDays(60).withStorageClass(StorageClass.Glacier))
        .withExpirationInDays(120)
        .withStatus(BucketLifecycleConfiguration.ENABLED);


    // apply this rules to the objects which has keyName tag and value - tagValue
    // LifecycleTagPredicate - to filter objects through tags
    BucketLifecycleConfiguration.Rule rule3=new Rule()
        .withId("rule id 1")
        .withFilter(new LifecycleFilter(new LifecycleTagPredicate(new Tag("keyName","tagValue"))))
        .addTransition(new Transition().withDays(60).withStorageClass(StorageClass.Glacier))
        .withExpirationInDays(120)
        .withStatus(BucketLifecycleConfiguration.ENABLED);

    // create BucketLifecycleConfiguration and set rule list
    BucketLifecycleConfiguration bucketLifecycleConfiguration=new BucketLifecycleConfiguration();
    bucketLifecycleConfiguration.setRules(Arrays.asList(rule,rule2,rule3));

    // save the configuration
    amazonS3.setBucketLifecycleConfiguration(bucketName,bucketLifecycleConfiguration);

  }

  public static void getLifeCycleConfiguration(AmazonS3 amazonS3,String bucketName){
    BucketLifecycleConfiguration bucketLifecycleConfiguration=amazonS3.getBucketLifecycleConfiguration(bucketName);
    List<BucketLifecycleConfiguration.Rule> rulesList=bucketLifecycleConfiguration.getRules();
    for(BucketLifecycleConfiguration.Rule rule:rulesList){
      LOG.info("bucket rule id- "+rule.getId());
    }
  }

  public static void deleteLifeCycleConfiguration(AmazonS3 amazonS3,String bucketName){
    amazonS3.deleteBucketLifecycleConfiguration(bucketName);
  }

}

//package com.lc.test;
//
//import com.amazonaws.services.s3.AmazonS3Client;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hdfs.DFSConfigKeys;
//import org.apache.hadoop.hdfs.HdfsConfiguration;
//import org.apache.hadoop.hdfs.protocol.Block;
//import org.apache.hadoop.hdfs.server.common.CloudHelper;
//import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderFactory;
//import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderS3Impl;
//import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.PartRef;
//import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.UploadID;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//
//public class TestCloudOperations {
//  public static void main(String argv[]) throws IOException {
//
//
//    Configuration conf = new HdfsConfiguration();
//    conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
//    conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, "aws");
//    conf.setBoolean(DFSConfigKeys.GCS_BUCKET_ENABLE_VERSIONING_KEY, true);
//    conf.set(DFSConfigKeys.DFS_CLOUD_AWS_S3_REGION, "us-east-2");
////   conf.set(DFSConfigKeys.GCS_BUCKET_REQUESTER_PAYS_PROJECT_KEY, "hopssalman");
//
//    //String bucket = "salman.ohio.1";
////    conf.setBoolean(DFSConfigKeys.AZURE_ENABLE_SOFT_DELETES_KEY, true);
////    conf.set(DFSConfigKeys.AZURE_CONTAINER_KEY, bucket);
////    conf.set(DFSConfigKeys.AZURE_STORAGE_KEY, "salmanstorageacc2");
////    conf.set(DFSConfigKeys.DFS_AZURE_MGM_IDENTITY_CLIENT_ID_KEY, "26d523c6-d25c-47ba-ae87-20e7e2c089ea");
//
//    conf.setBoolean(DFSConfigKeys.DFS_CLOUD_AWS_SERVER_SIDE_ENCRYPTION_BUCKET_KEY_ENABLE_KEY, true);
//
//    String bucket = argv[0];
//    System.out.println("Bucket : "+bucket);
//    conf.setBoolean(DFSConfigKeys.DFS_CLOUD_AWS_SERVER_SIDE_ENCRYPTION_ENABLE_KEY,true);
//
//    conf.set(DFSConfigKeys.DFS_CLOUD_AWS_SERVER_SIDE_ENCRYPTION_TYPE_KEY, argv[1]);
//    System.out.println("Enc Type : "+argv[1]);
//
//    if(argv[2].compareToIgnoreCase("null") == 0){
//      conf.set(DFSConfigKeys.DFS_CLOUD_AWS_SERVER_SIDE_ENCRYPTION_KEY_ARN_KEY, argv[2] );
//      System.out.println("ARN  : "+argv[2]);
//    }
//
//    Boolean owner = Boolean.parseBoolean(argv[3]);
//    conf.setBoolean(DFSConfigKeys.DFS_CLOUD_AWS_AMZ_ACL_BUCKET_OWNER_FULL_CONTROL_ENABLE_KEY, owner);
//
//
//    String objKey = CloudHelper.getBlockKey(1000, new Block(1, 1, 1, bucket));
//    String newKey = CloudHelper.getBlockKey(1000, new Block(2, 2, 2, bucket));
//
//    File objFile = new File("/tmp/" + UUID.randomUUID());
//    FileOutputStream fw = new FileOutputStream(objFile);
//    fw.write(new byte[1024 * 1024 * 10]);
//    fw.close();
//    File downFile = new File("/tmp/testfile");
//
//
//    conf.set(DFSConfigKeys.GCS_BUCKET_KEY, bucket);
//
//
//    List<String> buckets = new ArrayList();
//    buckets.add(bucket);
//
//    CloudPersistenceProviderS3Impl cloud =
//      (CloudPersistenceProviderS3Impl) CloudPersistenceProviderFactory.getCloudClient(conf);
//
//
////   try {
////     cloud.deleteAllBuckets("test");
////     System.out.println("PASS: Listintg all buckets ");
////   } catch( Exception e) {
////     System.out.println("FAIL: Listintg all buckets. "+e);
////   }
//
//
////   try {
////     cloud.format(buckets);
////     System.out.println("PASS: format ");
////   } catch( Exception e) {
////     System.out.println("FAIL: format. "+e);
////   }
//
////    AmazonS3Client s3Client = (AmazonS3Client)cloud.getCloudClient();
//
//    try {
//      cloud.checkAllBuckets(buckets);
//      System.out.println("PASS: Bucket check   ");
//    } catch (Exception e) {
//      System.out.println("FAIL: Bucket check " + e);
//    }
//
//
//    boolean exists = false;
//    //check if obj exists
//    try {
//      exists = cloud.objectExists(bucket, objKey);
//      System.out.println("PASS: Check obj exists ");
//    } catch (Exception e) {
//      System.out.println("FAIL: Check obj exists " + e);
//    }
//
//    if (!exists) {
//      try {
//        cloud.uploadObject(bucket, objKey, objFile, null);
//        System.out.println("PASS:  upload objcet ");
//      } catch (Exception e) {
//        System.out.println("FAIL:  upload objcet " + e);
//      }
//    }
//
//    //overwrite
//    try {
//      cloud.uploadObject(bucket, objKey, objFile, null);
//      System.out.println("PASS:  overwrite obj ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  overwrite obj " + e);
//    }
//
//    //read
//    try {
//      cloud.downloadObject(bucket, objKey, downFile);
//      System.out.println("PASS:  read obj ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  read obj " + e);
//    }
//
//    try {
//      cloud.getAll("/", buckets);
//      System.out.println("PASS:  listing the bucket objects ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  listing the bucket objects " + e);
//    }
//
//
//    try {
//      cloud.getUserMetaData(bucket, objKey);
//      System.out.println("PASS:  get obj metadata ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  get obj  metadata " + e);
//    }
//
//    try {
//      cloud.getObjectSize(bucket, objKey);
//      System.out.println("PASS:  get obj size ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  get obj  size " + e);
//    }
//
//
//    try {
//      cloud.getAllDirectories(buckets);
//      System.out.println("PASS:  get all dirs ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  get all dirs " + e);
//    }
//
//    //rename
//    try {
//      cloud.renameObject(bucket, bucket, objKey, newKey);
//      cloud.renameObject(bucket, bucket, newKey, objKey);
//      System.out.println("PASS:  rename  ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  rename " + e);
//    }
//
//    //copy obj
//    try {
//      cloud.copyObject(bucket, bucket, objKey, newKey, null);
//      System.out.println("PASS:  copy obj  ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  copy obj " + e);
//    }
//
//
//    //copy obj
//    try {
//      cloud.deleteObject(bucket, newKey);
//      System.out.println("PASS:  delete  ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  delete " + e);
//    }
//
//    //multipart
//    UploadID uid = null;
//    try {
//      uid = cloud.startMultipartUpload(bucket, newKey, null);
//      System.out.println("PASS:  start multipart  ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  start multipart " + e);
//    }
//
//    //upload part
//    ArrayList<PartRef> prefs = new ArrayList<PartRef>();
//    try {
//      prefs.add(cloud.uploadPart(bucket, newKey, uid, 1, objFile, 0, objFile.length()));
//      System.out.println("PASS:  upload part   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  upload part " + e);
//      e.printStackTrace();
//    }
//
//    //finalize part
//    try {
//      cloud.finalizeMultipartUpload(bucket, newKey, uid, prefs);
//      System.out.println("PASS:  finalize part   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  finalize part " + e);
//    }
//
//    //finalize part
//    try {
//      cloud.deleteObject(bucket, newKey);
//      System.out.println("PASS:  delete   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  delete " + e);
//    }
//
//
//    // testing abort
//    try {
//      uid = cloud.startMultipartUpload(bucket, newKey, null);
//      System.out.println("PASS:  start multipart  ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  start multipart " + e);
//    }
//
//    //upload part
//    prefs.clear();
//    try {
//      prefs.add(cloud.uploadPart(bucket, newKey, uid, 1, objFile, 0, objFile.length()));
//      System.out.println("PASS:  upload part   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  upload part " + e);
//    }
//
//    //abort
//    try {
//      cloud.abortMultipartUpload(bucket, newKey, uid);
//      System.out.println("PASS:  abort multipart   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  abort multipart " + e);
//    }
//
//    //list multipart
//    try {
//      cloud.listMultipartUploads(buckets, "/");
//      System.out.println("PASS:  list multipart upload   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  list multipart upload " + e);
//    }
//
//
//    //delete block
//    try {
//      cloud.deleteObject(bucket, objKey);
//      System.out.println("PASS:  delete   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  delete " + e);
//    }
//
//    //restore block
//    try {
//      if (!cloud.restoreDeletedBlock(bucket, objKey)) {
//        System.out.println("FAIL:  restore did not work ");
//      } else
//        System.out.println("PASS:  restore   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  restore " + e);
//    }
//
//    //is versionin
//    try {
//      if (!cloud.isVersioningSupported(bucket)) {
//        System.out.println("FAIL:  is version supported is false  ");
//      } else
//        System.out.println("PASS:  versioning supported   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  versioning supported " + e);
//    }
//
//    //overwrite to create a new version
//    try {
//      cloud.uploadObject(bucket, objKey, objFile, null);
//      System.out.println("PASS:  upload objcet ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  upload objcet " + e);
//    }
//
//    //delete all version
//    try {
//      cloud.deleteOldVersions(bucket, objKey);
//      System.out.println("PASS:  delete old versions   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  delete old version " + e);
//    }
//
//
//    //delete all version
//    try {
//      cloud.deleteAllVersions(bucket, objKey);
//      System.out.println("PASS:  delete all versions   ");
//    } catch (Exception e) {
//      System.out.println("FAIL:  delete all version " + e);
//    }
//    System.exit(0);
//  }
//}
//
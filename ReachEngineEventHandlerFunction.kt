package fandor

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.event.S3EventNotification
import io.micronaut.function.FunctionBean
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.function.Consumer
import com.amazonaws.regions.Regions
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.amazonaws.services.sns.model.PublishRequest
import io.micronaut.context.annotation.Property
import java.io.InputStream


//import AWS from 'aws-sdk'

@FunctionBean("reach-engine-event-handler")
class ReachEngineEventHandlerFunction(val fileRepository: FileRepo, val topicGateway: TopicGateway) : Consumer<S3EventNotification> {

    val LOG: Logger = LoggerFactory.getLogger(ReachEngineEventHandlerFunction::class.java)

    override fun accept(event: S3EventNotification) {

        LOG.debug("event: ${event}")
        val lastRecord = event.records.last() // have to get a record from the records list

        val textEventOutput = fileRepository.getFileContent(lastRecord.s3.bucket.name,lastRecord.s3.`object`.key)//get file

        topicGateway.post(textEventOutput)

    }

}

fun transformInputStreamToText(input: InputStream): String {
    // Read the text input stream one line at a time and display each line.
    val inputAsString = input.bufferedReader().use { it.readText() }
    return inputAsString
}

interface FileRepo {
    fun getFileContent(directory: String, fileName:String): String
}

class S3FileRepo(@Property(name = "clients.s3.name") val region: String) : FileRepo {

    override fun getFileContent(directory: String, fileName:String): String {
        val LOG = LoggerFactory.getLogger(ReachEngineEventHandlerFunction::class.java)

        val s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build()
        val payloadObject = s3Client.getObject(directory, fileName)
        return transformInputStreamToText(payloadObject.objectContent)

    }
}

interface TopicGateway {
    fun post(message: String)
}

class SNSTopicGateway : TopicGateway {
    override fun post(message: String) {
        val LOG: Logger = LoggerFactory.getLogger(ReachEngineEventHandlerFunction::class.java)
        val snsClient = AmazonSNSClientBuilder.standard().withRegion(Regions.US_EAST_1).build()

        val topicArn = "arn:aws:sns:us-east-1:862208614941:update-film"
        val publishRequest = PublishRequest(topicArn, message)
        val publishResult = snsClient.publish(publishRequest)
        //print MessageId of message published to SNS topic
        LOG.debug("MessageId - " + publishResult.getMessageId())
    }
}

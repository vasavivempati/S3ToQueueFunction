package fandor

import com.amazonaws.services.s3.event.S3EventNotification
import io.mockk.mockk
import io.mockk.verify
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.io.ByteArrayInputStream
import java.io.InputStream

object ReachEngineEventHandlerFunctionTest : Spek({

    describe("BookController Suite") {
        describe("test accept method") {
            val fileRepoMock = mockk<FileRepo>(relaxed = true)
            val topicGatewayMock = mockk<TopicGateway>(relaxed = true)
            val testObject = ReachEngineEventHandlerFunction(fileRepoMock, topicGatewayMock)

            val entity: S3EventNotification.RequestParametersEntity = S3EventNotification.RequestParametersEntity("70.90.214.57")
            val elements: S3EventNotification.ResponseElementsEntity = S3EventNotification.ResponseElementsEntity("915BAA865A1F12D1", "QNnAHIOJAOUq25d9ah/95ZHHh8T4KGNhRXA+UMTXFMBoe1FnTCatCFg0obm9wLPbdxlm6tnSDdI=")
            val id: S3EventNotification.UserIdentityEntity = S3EventNotification.UserIdentityEntity("7")
            val notification: S3EventNotification.S3BucketEntity = S3EventNotification.S3BucketEntity("my_bucket", id, "arn")
            val objectEntity: S3EventNotification.S3ObjectEntity = S3EventNotification.S3ObjectEntity("my_file", 7, "", "")
            val s3Entity: S3EventNotification.S3Entity = S3EventNotification.S3Entity("PutEvent", notification, objectEntity, "s3schemaVersion")
            val record: S3EventNotification.S3EventNotificationRecord = S3EventNotification.S3EventNotificationRecord("US East (N. Virginia)", "ObjectCreated:Put", "aws:s3", "2018-09-24T21:15:10.404Z", "2.0", entity, elements, s3Entity, id)
            record.s3.`object`.key

            val records: List<S3EventNotification.S3EventNotificationRecord> = mutableListOf(record, record)//mutableListOf(record, record)

            val event = S3EventNotification(records)
            println("this is event ")
            println(event.records.last())
            testObject.accept(event)
            it("tests ReachEngineEventHandlerFunction gets & accepts fileRepository") {
                verify { fileRepoMock.getFileContent(record.s3.bucket.name,record.s3.`object`.key) }
            }
            it("tests ReachEngineEventHandlerFunction gets & accepts topicGateway") {
               	 val  fileName = record.s3.`object`.key
		 val  directoryName = record.s3.bucket.name
		 val fileContent = fileRepoMock.getFileContent(directoryName,fileName)
		 verify { topicGatewayMock.post(fileContent) }
            }
        }

        describe(" test I/O stream converter ") {
            it("test I/O Stream Test ") {
                val input = "Hello there!"
                val inputStream = ByteArrayInputStream(input.toByteArray(Charsets.UTF_8))
                val output = transformInputStreamToText(inputStream)
                assert( output.equals("Hello there!"))
            }
        }

    }
})

package net.devcat.avro.model;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

public class UserTest {

    private static final Logger LOGGER = LogManager .getLogger(UserTest.class);
    private static final File outputFile = new File("target/avro/user.avro");

    @BeforeClass
    public static void setup() {

        LOGGER.info("Starting User serializer example...");
		
        if (outputFile.exists()) {
			outputFile.delete();
        }
		
        new File("target/avro").mkdir();
        
        User user = User.newBuilder()
            .setFirstName("John")
            .setLastName("Blow")
            .setCity("San Jose")
            .setState("CA")
            .build();

        DatumWriter<User> datumWriter = 
            new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> fileWriter = 
            new DataFileWriter<User>(datumWriter);
        try {
            fileWriter.create(user.getSchema(), outputFile);
            fileWriter.append(user);
            fileWriter.close();
        } catch (IOException e) {
            LOGGER.error("Error while trying to write the object to file <"
            + outputFile.getAbsolutePath() + ">.", e);
        }
            LOGGER.info("Finished running User serializer example.");
    }

    @Test
    public void testDeserialize() {
        DatumReader<User> datumReader = 
            new SpecificDatumReader<User>(User.class);
        try {
            DataFileReader<User> fileReader = 
                new DataFileReader<User>(outputFile, datumReader);
            User user = null;
			
            if (fileReader.hasNext()) {
                user = fileReader.next(user);
            }
            assertEquals("John", user.getFirstName().toString());
            assertEquals("Blow", user.getLastName().toString());
            assertEquals("San Jose", user.getCity().toString());
            assertEquals("CA", user.getState().toString());
        } catch (IOException e) {
            LOGGER.error("Error while trying to read the object from file <"
                + outputFile.getAbsolutePath() + ">.", e);
        }
    }
}


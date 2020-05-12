package seminar;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class DataProcessor implements RequestHandler<S3Event, String> {

	private final static String CSV_URL = "https://raw.githubusercontent.com/stritti/thermal-solar-plant-dataset/master/data/2019/07/20190723.csv";
	private final static String TARGET_BUCKET_NAME = "thermal-solar-plant";

	@Override
	public String handleRequest(S3Event s3Event, Context context) {

		try (final InputStream inputStream = new BufferedInputStream(new URL(CSV_URL).openStream())) {
			final List<CSVRecord> records =
					CSVParser.parse(inputStream, Charset.defaultCharset(), CSVFormat.TDF.withFirstRecordAsHeader().withTrim()).getRecords();

			System.out.println("#records in csv: " + records.size());
			System.out.println("first record: " + records.get(0).toString());

			double temperatureAvg = getTemperatureAvg(records);
			double standardDeviation = getTemperatureStandardDeviation(records, temperatureAvg);

			final String output = "Average temperatur: " + temperatureAvg + ", standard deviation " + standardDeviation;
			writeStringToS3Bucket(output);


		} catch (final Exception e) {
			System.out.println("Error reading data set");
			e.printStackTrace();
		}

		return "Success; number of events: " + s3Event.getRecords().size();
	}

	private static void writeStringToS3Bucket(final String output) {
		final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

		System.out.println("Writing output " + output + " to S3 Bucket " + TARGET_BUCKET_NAME);
		s3Client.putObject(TARGET_BUCKET_NAME, "output", output);

		s3Client.shutdown();
	}

	private static double getTemperatureStandardDeviation(final List<CSVRecord> records, final double temperatureAvg) {
		double temperatureSumSquared = 0;

		for (final CSVRecord record : records) {
			temperatureSumSquared += Math.pow(parseDouble(record) - temperatureAvg, 2);
		}

		return Math.sqrt(temperatureSumSquared / (double) records.size());
	}

	private static double getTemperatureAvg(final List<CSVRecord> records) {
		double temperatureSum = 0;

		for (final CSVRecord record : records) {
			temperatureSum += parseDouble(record);
		}

		return temperatureSum / (double) records.size();
	}

	private static double parseDouble(final CSVRecord record) {
		return Double.parseDouble(record.get(1).replace(',', '.'));
	}
}

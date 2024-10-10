## TableStore-Migration-Tool

TableStore-Migration-Tool is a **Spring Boot** application designed to facilitate the migration of data from **AliCloud TableStore** to **AWS DynamoDB**,**MongoDB** or **AWS S3** using the TableStore Tunnel Service. This application supports both full data migration and incremental data migration, making it a robust solution for data migration needs.

#### Features

- **Full Data Migration**: Migrate all data from TableStore to DynamoDB or S3.
- **Incremental Data Migration**: Migrate only the changes made through TableStore Tunnel Service.
- **Configurable**: Easy setup through configuration file.

#### Prerequisites

- Java 8 or higher
- Maven
- Access to AliCloud and AWS accounts

#### Configuration

Before running the application, you need to configure your AliCloud TableStore and AWS DynamoDB/MongoDB/S3 settings. Edit the `application.properties` file located in `config/` with your specific configurations:

```properties
# TableStore configs
ots.migration.config.sourceEndPoint=https://xxxxxx.cn-hangzhou.ots.aliyuncs.com
ots.migration.config.accessKeyId=xxxxxxxxxxxxxxxx
ots.migration.config.accessKeySecret=xxxxxxxxxxxxxxxxxxxxxx
ots.migration.config.instanceName=xxxxx
ots.migration.config.tableName=xxxxx
ots.migration.config.tunnelName=xxxx

# DynamoDB configs
ots.migration.config.targetEndpoint=dynamodb.cn-northwest-1.amazonaws.com.cn
```

#### Compile and Deploy

To compile and package the application, run the following command in your terminal:

```bash
mvn clean install
```

This will generate a JAR file in the `target` directory.

#### Run the Application

You can run the application using the following command:

```bash
java -Dspring.config.additional-location=config/ -jar target/ots-mgr-0.0.1-SNAPSHOT.jar
```

Ensure that all configurations are correctly set before executing this command.

#### Contribution

Contributions are welcome! Please feel free to submit issues and pull requests.

#### Security

See CONTRIBUTING for more information.

#### License

This library is licensed under the MIT-0 License. See the LICENSE file.
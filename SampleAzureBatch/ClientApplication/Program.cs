using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using TaskApplication;
using System.Data.SqlClient;
using System.Text;

namespace ClientApplication
{
    public class Program
    {
        #region Fields
        // Batch account credentials
        private const string BatchAccountName = "samplebatch24oct";
        private const string BatchAccountKey = "bBsmk9CI3QHwbxCwf6QgAitirPEsAoeW9Z2cnGGDYxA1fyOsb25Mub6g4wfBEXRCNWtMr66D2aM5koKBVMJ2nA==";
        private const string BatchAccountUrl = "https://samplebatch24oct.westindia.batch.azure.com";

        // Storage account credentials
        private const string StorageAccountName = "samplebatch24oct";
        private const string StorageAccountKey = "c9/f2ePMxp8OCDRQ1QCbtVOfof55Ae6Grfm1ySkLbqr15XBQdKj4Ku5DjDp9vbyZzKGjeEA9FZgZDcgj5c5L1A==";

        private const string PoolId = "SampleAzureBatchPool";
        private const string JobId = "SampleAzureBatchJob"; 
        #endregion

        #region Main
        public static void Main(string[] args)
        {
            if (String.IsNullOrEmpty(BatchAccountName) || String.IsNullOrEmpty(BatchAccountKey) || String.IsNullOrEmpty(BatchAccountUrl) ||
                String.IsNullOrEmpty(StorageAccountName) || String.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("One ore more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }

            try
            {
                // Call the asynchronous version of the Main() method. This is done so that we can await various
                // calls to async methods within the "Main" method of this console application.
                MainAsync().Wait();
            }
            catch (AggregateException ae)
            {
                Console.WriteLine();
                Console.WriteLine("One or more exceptions occurred.");
                Console.WriteLine();

                PrintAggregateException(ae);
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
        }

        /// <summary>
        /// Provides an asynchronous version of the Main method, allowing for the awaiting of async method calls within.
        /// </summary>
        /// <returns>A Task object that represents the asynchronous operation.</returns>
        private static async Task MainAsync()
        {
            #region Initial Setup
            Console.WriteLine("Sample start: {0}", DateTime.Now);
            Console.WriteLine();

            InsertIntoDatabase("Batch started at : " + DateTime.Now);

            Stopwatch timer = new Stopwatch();
            timer.Start();
            #endregion

            #region Creating Storage Containers
            // Construct the Storage account connection string
            string storageConnectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                                                            StorageAccountName, StorageAccountKey);

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client, for use in obtaining references to blob storage containers
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Use the blob client to create the containers in Azure Storage if they don't yet exist
            const string appContainerName = "application";
            const string inputContainerName = "input";
            const string outputContainerName = "output";
            await CreateContainerIfNotExistAsync(blobClient, appContainerName);
            await CreateContainerIfNotExistAsync(blobClient, inputContainerName);
            await CreateContainerIfNotExistAsync(blobClient, outputContainerName);
            #endregion

            #region Upload Data and Application Files
            // Paths to the executable and its dependencies that will be executed by the tasks
            List<string> applicationFilePaths = new List<string>
            {
                // The ClientApplication project includes a project reference to TaskApplication, allowing us to
                // determine the path of the task application binary dynamically
                typeof(TaskApplication.Program).Assembly.Location,
                "ABCD.dll",
                "Microsoft.WindowsAzure.Storage.dll"
            };

            // The collection of data files that are to be processed by the tasks
            List<string> inputFilePaths = new List<string>
            {
                @"..\..\taskdata1.txt",
                @"..\..\taskdata2.txt",
                @"..\..\taskdata3.txt"
            };

            // Upload the application and its dependencies to Azure Storage. This is the application that will
            // process the data files, and will be executed by each of the tasks on the compute nodes.
            List<ResourceFile> applicationFiles = await UploadFilesToContainerAsync(blobClient, appContainerName, applicationFilePaths);

            // Upload the data files. This is the data that will be processed by each of the tasks that are
            // executed on the compute nodes within the pool.
            List<ResourceFile> inputFiles = await UploadFilesToContainerAsync(blobClient, inputContainerName, inputFilePaths);
            #endregion

            #region Retrieving Output SasUrl
            // Obtain a shared access signature that provides write access to the output container to which
            // the tasks will upload their output.
            string outputContainerSasUrl = GetContainerSasUrl(blobClient, outputContainerName, SharedAccessBlobPermissions.Write);
            #endregion

            #region Managing Batch, Job and Tasks
            // Create a BatchClient. We'll now be interacting with the Batch service in addition to Storage
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                // Create the pool that will contain the compute nodes that will execute the tasks.
                // The ResourceFile collection that we pass in is used for configuring the pool's StartTask
                // which is executed each time a node first joins the pool (or is rebooted or reimaged).
                await CreatePoolIfNotExistAsync(batchClient, PoolId, applicationFiles);

                // Create the job that will run the tasks.
                await CreateJobAsync(batchClient, JobId, PoolId);

                // Add the tasks to the job. We need to supply a container shared access signature for the
                // tasks so that they can upload their output to Azure Storage.
                await AddTasksAsync(batchClient, JobId, inputFiles, outputContainerSasUrl);

                // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete
                await MonitorTasks(batchClient, JobId, TimeSpan.FromMinutes(30));

                // Download the task output files from the output Storage container to a local directory
                await DownloadBlobsFromContainerAsync(blobClient, outputContainerName, Environment.GetEnvironmentVariable("TEMP"));

                // Clean up Storage resources
                //await DeleteContainerAsync(blobClient, appContainerName);
                //await DeleteContainerAsync(blobClient, inputContainerName);
                //await DeleteContainerAsync(blobClient, outputContainerName);

                // Print out some timing info
                timer.Stop();
                Console.WriteLine();
                Console.WriteLine("Sample end: {0}", DateTime.Now);
                Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

                InsertIntoDatabase("Batch ended at : " + DateTime.Now);

                // Clean up Batch resources (if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    await batchClient.JobOperations.DeleteJobAsync(JobId);
                }

                //Console.Write("Delete pool? [yes] no: ");
                //response = Console.ReadLine().ToLower();
                //if (response != "n" && response != "no")
                //{
                //    await batchClient.PoolOperations.DeletePoolAsync(PoolId);
                //}
            }
            #endregion
        } 
        #endregion

        #region Factory methods for creating Storage Container
        /// <summary>
        /// Creates a container with the specified name in Blob storage, unless a container with that name already exists.
        /// </summary>
        /// <param name="blobClient"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
        private static async Task CreateContainerIfNotExistAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (await container.CreateIfNotExistsAsync())
            {
                Console.WriteLine("Container [{0}] created.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] exists, skipping creation.", containerName);
            }
        }
        #endregion

        #region Factory method for OutputContainer's Sas Url
        /// <summary>
        /// Returns a shared access signature (SAS) URL providing the specified permissions to the specified container.
        /// </summary>
        /// <param name="blobClient">A CloudBlobClient.</param>
        /// <param name="containerName">The name of the container for which a SAS URL should be obtained.</param>
        /// <param name="permissions">The permissions granted by the SAS URL.</param>
        /// <returns>A SAS URL providing the specified access to the container.</returns>
        /// <remarks>The SAS URL provided is valid for 2 hours from the time this method is called. The container must
        /// already exist within Azure Storage.</remarks>
        private static string GetContainerSasUrl(CloudBlobClient blobClient, string containerName, SharedAccessBlobPermissions permissions)
        {
            // Set the expiry time and permissions for the container access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = permissions
            };

            // Generate the shared access signature on the container, setting the constraints directly on the signature
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

            // Return the URL string for the container, including the SAS token
            return String.Format("{0}{1}", container.Uri, sasContainerToken);
        }
        #endregion

        #region Factory methods for uploading files to Storage containers
        /// <summary>
        /// Uploads the specified files to the specified Blob container, returning a corresponding
        /// collection of ResourceFile objects appropriate for assigning to a task's
        /// </summary>
        /// <param name="blobClient">A CloudBlobClient.</param>
        /// <param name="inputContainerName">The name of the blob storage container to which the files should be uploaded.</param>
        /// <param name="filePaths">A collection of paths of the files to be uploaded to the container.</param>
        /// <returns>A collection of ResourceFile objects.</returns>
        private static async Task<List<ResourceFile>> UploadFilesToContainerAsync(CloudBlobClient blobClient, string inputContainerName, List<string> filePaths)
        {
            List<ResourceFile> resourceFiles = new List<ResourceFile>();

            foreach (string filePath in filePaths)
            {
                resourceFiles.Add(await UploadFileToContainerAsync(blobClient, inputContainerName, filePath));
            }

            return resourceFiles;
        }

        /// <summary>
        /// Uploads the specified file to the specified Blob container.
        /// </summary>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <param name="blobClient">A CloudBlobClient.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <returns>A  ResourceFile instance representing the file within blob storage.</returns>
        private static async Task<ResourceFile> UploadFileToContainerAsync(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            await blobData.UploadFromFileAsync(filePath);

            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return new ResourceFile(blobSasUri, blobName);
        }
        #endregion

        #region Factory method for creating Batch pool
        /// <summary>
        /// Creates a CloudPool with the specified id and configures its StartTask with the
        /// specified ResourceFile collection.
        /// </summary>
        /// <param name="batchClient">A BatchClient.</param>
        /// <param name="poolId">The id of the CloudPool to create.</param>
        /// <param name="resourceFiles">A collection of ResourceFile objects representing blobs within
        /// a Storage account container. The StartTask will download these files from Storage prior to execution.</param>
        /// <returns>A Task object that represents the asynchronous operation.</returns>
        private static async Task CreatePoolIfNotExistAsync(BatchClient batchClient, string poolId, IList<ResourceFile> resourceFiles)
        {
            CloudPool pool = null;
            try
            {
                Console.WriteLine("Creating pool [{0}]...", poolId);

                // Create the unbound pool. Until we call CloudPool.Commit() or CommitAsync(), no pool is actually created in the
                // Batch service. This CloudPool instance is therefore considered "unbound," and we can modify its properties.
                pool = batchClient.PoolOperations.CreatePool(
                    poolId: poolId,
                    targetDedicatedComputeNodes: 3,                                             // 3 compute nodes
                    virtualMachineSize: "small",                                                // single-core, 1.75 GB memory, 225 GB disk
                    cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"));   // Windows Server 2012 R2

                // Create and assign the StartTask that will be executed when compute nodes join the pool.
                // In this case, we copy the StartTask's resource files (that will be automatically downloaded
                // to the node by the StartTask) into the shared directory that all tasks will have access to.
                pool.StartTask = new StartTask
                {
                    // Specify a command line for the StartTask that copies the task application files to the
                    // node's shared directory. Every compute node in a Batch pool is configured with a number
                    // of pre-defined environment variables that can be referenced by commands or applications
                    // run by tasks.

                    // Since a successful execution of robocopy can return a non-zero exit code (e.g. 1 when one or
                    // more files were successfully copied) we need to manually exit with a 0 for Batch to recognize
                    // StartTask execution success.
                    CommandLine = "cmd /c (robocopy %AZ_BATCH_TASK_WORKING_DIR% %AZ_BATCH_NODE_SHARED_DIR%) ^& IF %ERRORLEVEL% LEQ 1 exit 0",
                    ResourceFiles = resourceFiles,
                    WaitForSuccess = true
                };

                await pool.CommitAsync();
            }
            catch (BatchException be)
            {
                // Swallow the specific error code PoolExists since that is expected if the pool already exists
                if (be.RequestInformation?.BatchError != null && be.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", poolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }
        #endregion

        #region Factory method for creating Batch Job
        /// <summary>
        /// Creates a job in the specified pool.
        /// </summary>
        /// <param name="batchClient">A BatchClient.</param>
        /// <param name="jobId">The id of the job to be created.</param>
        /// <param name="poolId">The id of the CloudPool in which to create the job.</param>
        /// <returns>A Task object that represents the asynchronous operation.</returns>
        private static async Task CreateJobAsync(BatchClient batchClient, string jobId, string poolId)
        {
            Console.WriteLine("Creating job [{0}]...", jobId);

            CloudJob job = batchClient.JobOperations.CreateJob();
            job.Id = jobId;
            job.PoolInformation = new PoolInformation { PoolId = poolId };

            await job.CommitAsync();
        }
        #endregion

        #region Factory methods for managing Tasks
        /// <summary>
        /// Creates tasks to process each of the specified input files, and submits them to the
        /// specified job for execution.
        /// </summary>
        /// <param name="batchClient">A BatchClient.</param>
        /// <param name="jobId">The id of the job to which the tasks should be added.</param>
        /// <param name="inputFiles">A collection of ResourceFile objects representing the input files to be
        /// processed by the tasks executed on the compute nodes.</param>
        /// <param name="outputContainerSasUrl">The shared access signature URL for the container within Azure Storage that
        /// will receive the output files created by the tasks.</param>
        /// <returns>A collection of the submitted tasks.</returns>
        private static async Task<List<CloudTask>> AddTasksAsync(BatchClient batchClient, string jobId, List<ResourceFile> inputFiles, string outputContainerSasUrl)
        {
            Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, jobId);

            // Create a collection to hold the tasks that we'll be adding to the job
            List<CloudTask> tasks = new List<CloudTask>();

            // Create each of the tasks. Because we copied the task application to the
            // node's shared directory with the pool's StartTask, we can access it via
            // the shared directory on whichever node each task will run.
            foreach (ResourceFile inputFile in inputFiles)
            {
                string taskId = "topNtask" + inputFiles.IndexOf(inputFile);
                string taskCommandLine = String.Format("cmd /c %AZ_BATCH_NODE_SHARED_DIR%\\TaskApplication.exe {0} 3 \"{1}\"", inputFile.FilePath, outputContainerSasUrl);

                CloudTask task = new CloudTask(taskId, taskCommandLine);
                task.ResourceFiles = new List<ResourceFile> { inputFile };
                tasks.Add(task);
            }

            // Add the tasks as a collection opposed to a separate AddTask call for each. Bulk task submission
            // helps to ensure efficient underlying API calls to the Batch service.
            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);

            return tasks;
        }

        /// <summary>
        /// Monitors the specified tasks for completion and returns a value indicating whether all tasks completed successfully
        /// within the timeout period.
        /// </summary>
        /// <param name="batchClient">A BatchClient </param>
        /// <param name="jobId">The id of the job containing the tasks that should be monitored.</param>
        /// <param name="timeout">The period of time to wait for the tasks to reach the completed state.</param>
        /// <returns><c>true</c> if all tasks in the specified job completed with an exit code of 0 within the specified timeout period, otherwise <c>false</c>.</returns>
        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            bool allTasksSuccessful = true;
            const string successMessage = "All tasks reached state Completed.";
            const string failureMessage = "One or more tasks failed to reach the Completed state within the timeout period.";

            // Obtain the collection of tasks currently managed by the job. Note that we use a detail level to
            // specify that only the "id" property of each task should be populated. Using a detail level for
            // all list operations helps to lower response time from the Batch service.
            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");
            List<CloudTask> tasks = await batchClient.JobOperations.ListTasks(JobId, detail).ToListAsync();

            Console.WriteLine("Awaiting task completion, timeout in {0}...", timeout.ToString());

            // We use a TaskStateMonitor to monitor the state of our tasks. In this case, we will wait for all tasks to
            // reach the Completed state.
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
            try
            {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            }
            catch (TimeoutException)
            {
                await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);
                Console.WriteLine(failureMessage);
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, successMessage);

            // All tasks have reached the "Completed" state, however, this does not guarantee all tasks completed successfully.
            // Here we further check each task's ExecutionInfo property to ensure that it did not encounter a scheduling error
            // or return a non-zero exit code.

            // Update the detail level to populate only the task id and executionInfo properties.
            // We refresh the tasks below, and need only this information for each task.
            detail.SelectClause = "id, executionInfo";

            foreach (CloudTask task in tasks)
            {
                // Populate the task's properties with the latest info from the Batch service
                await task.RefreshAsync(detail);

                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure)
                {
                    // A task with failure information set indicates there was a problem with the task. It is important to note that
                    // the task's state can be "Completed," yet still have encountered a failure.

                    allTasksSuccessful = false;

                    Console.WriteLine("WARNING: Task [{0}] encountered a failure: {1}", task.Id, task.ExecutionInformation.FailureInformation.Message);
                    if (task.ExecutionInformation.ExitCode != 0)
                    {
                        // A non-zero exit code may indicate that the application executed by the task encountered an error
                        // during execution. As not every application returns non-zero on failure by default (e.g. robocopy),
                        // your implementation of error checking may differ from this example.

                        Console.WriteLine("WARNING: Task [{0}] returned a non-zero exit code - this may indicate task execution or completion failure.", task.Id);
                    }
                }
            }

            if (allTasksSuccessful)
            {
                Console.WriteLine("Success! All tasks completed successfully within the specified timeout period.");
            }

            return allTasksSuccessful;
        }
        #endregion

        #region Factory methods for deleting files and Storage containers
        /// <summary>
        /// Deletes the container with the specified name from Blob storage, unless a container with that name does not exist.
        /// </summary>
        /// <param name="blobClient">A CloudBlobClient.</param>
        /// <param name="containerName">The name of the container to delete.</param>
        /// <returns>A Task object that represents the asynchronous operation.</returns>
        private static async Task DeleteContainerAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (await container.DeleteIfExistsAsync())
            {
                Console.WriteLine("Container [{0}] deleted.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] does not exist, skipping deletion.", containerName);
            }
        }

        /// <summary>
        /// Downloads all files from the specified blob storage container to the specified directory.
        /// </summary>
        /// <param name="blobClient">A CloudBlobClient </param>
        /// <param name="containerName">The name of the blob storage container containing the files to download.</param>
        /// <param name="directoryPath">The full path of the local directory to which the files should be downloaded.</param>
        /// <returns>A Task object that represents the asynchronous operation.</returns>
        private static async Task DownloadBlobsFromContainerAsync(CloudBlobClient blobClient, string containerName, string directoryPath)
        {
            Console.WriteLine("Downloading all files from container [{0}]...", containerName);

            // Retrieve a reference to a previously created container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Get a flat listing of all the block blobs in the specified container
            foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
            {
                // Retrieve reference to the current blob
                CloudBlob blob = (CloudBlob)item;

                // Save blob contents to a file in the specified folder
                string localOutputFile = Path.Combine(directoryPath, blob.Name);
                await blob.DownloadToFileAsync(localOutputFile, FileMode.Create);
            }

            Console.WriteLine("All files downloaded to {0}", directoryPath);
        }
        #endregion

        #region Util method
        /// <summary>
        /// Processes all exceptions inside an AggregateException and writes each inner exception to the console.
        /// </summary>
        /// <param name="aggregateException">The AggregateException to process.</param>
        public static void PrintAggregateException(AggregateException aggregateException)
        {
            // Flatten the aggregate and iterate over its inner exceptions, printing each
            foreach (Exception exception in aggregateException.Flatten().InnerExceptions)
            {
                Console.WriteLine(exception.ToString());
                Console.WriteLine();
            }
        }

        public static void InsertIntoDatabase(string value)
        {
            try
            {
                string connectionString = "Data Source=metademo.japaneast.cloudapp.azure.com; Initial Catalog=DIVA; Integrated Security=False;User ID=metadbadmin;Password=MetaCiitdc#2017;";

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    Console.WriteLine("\nQuery data example:");
                    Console.WriteLine("=========================================\n");

                    connection.Open();
                    StringBuilder sb = new StringBuilder();
                    sb.Append("Insert into dbo.Sample1");
                    sb.Append(" Values ('" + value + "')");
                    String sql = sb.ToString();

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine("{0} {1}", reader.GetString(0), reader.GetString(1));
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        #endregion
    }
}

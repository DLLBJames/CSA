package com.ibm.isa.oppty.isa_oppty;

import org.apache.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * Hello world!
 *
 */
public class App {
	static final Logger log = Logger.getLogger(App.class);
    public static void main( String[] args ) throws Exception{
    	App obj = new App();
		obj.run();
	}
		private void run() throws Exception {

			String[] springConfig = { "spring/batch/jobs/job-oppty.xml" };

			ApplicationContext context = new ClassPathXmlApplicationContext(springConfig);

			JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
			Job job = (Job) context.getBean("opptyJob");
			
			CloudantDataAccess dataAccess = (CloudantDataAccess) context.getBean("CloudantDataAccess");
			
			boolean runornot = dataAccess.dbExists("oppty");
			if (runornot){
				JobParametersBuilder jpb = new JobParametersBuilder();
				jpb.addString("job_detail","oppty_clean");
				JobParameters jobParams = jpb.toJobParameters();
				JobExecution execution = jobLauncher.run(job, jobParams);
				log.info("Exit Status : " + execution.getStatus());
				log.info("Exit Status : " + execution.getAllFailureExceptions());
				log.info("Done");
			}else{
				log.info("cloudant---oppty table is not exist");
			}
			
			

		}
		
}

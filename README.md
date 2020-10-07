# Engineering_challenge
commands to build
*****************
	cd <project_root>
	docker build -t"dl_inst:0.0.2" . | tee tests/test_docker_build_out.txt
	docker run 68e9061e57d4 | tee tests/test_app_run_output_from_docker.txt

****NOTE : It is optional to use tee to redirect to output to stdin and to a file. replace "test" directory to "production log" directory.

Engineering_challenge
********************

Engineering assumptions:

	1. average temptation between D & N (0400 - 1600) derived to bench mark a day's temperature
	2. any day of month greater this benchmark is what the engineered process  identifies as "hottest day of month"
	3. row group date/days implemented

Tranformations

	1. screen temperature -99 is replaces to Nan (looks to be a logical non empty value from source)
	2. missing temperature is populated with previous & next temperature
		1200 -> 10, 1300 -> Nan, 1500 -> 20, 1400 -> will be 15 degree
	3. when most values or missing in a group, average is calculated be sum_value by nine
		1st of March for site code 3227 has temperature value of 13.6 at 0800, temperature is not available for rest of the 
		day till 1600; being the highest temperature in entire period average drops.
	4. missing country values populated based from similar region with values derived from the group.
	5. formatted time as required

Data cleansing

	1. time part stripped from observation date

Modelling

	1. for site & location dimension table engineered, using hash keys
	
	2. fact tables for each month, class module do insert foreign key to fact entity.
	
	3. fact aggregate to be referenced for output; each row is a combination of site & date with two cols with average temp
		(avg_tem) & dictionary column with all possible temperature between 0800-1600 (temperature)

Testing

	1. have done a limited testing to prove that this aspect is also catered in limited time while engineering solutions.
		it was done sparingly with few csv files.
		
	2. more testing to be done for every method which will be an exhaustive feature on its own

DevOps

	1. As part of the challenge to establish familiarity in dev-ops and governance relating to production deployment where
	
		docker images with tags will be released.  I have built a docker image, same have been tested to host the application
		
		results of docker build success, image name etc have been added as evidences to the folder.
		

"test folder" contents


	1. test folder -> have input csv file and expected csv files
	
	2. docker build and running the solutions, output is redirected and available to view.
		1. test_app_run_output_from_docker.txt, test_docker_build_out.txt
	3. test script run results evidence in test_results.txt


		

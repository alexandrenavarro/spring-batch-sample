package hello;

import de.codecentric.boot.admin.config.EnableAdminServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.time.LocalDateTime;

@Configuration
@EnableBatchProcessing
@EnableScheduling
@EnableAutoConfiguration
@EnableAdminServer
@Slf4j
public class BatchConfiguration implements CommandLineRunner {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Autowired
    public JobLauncher jobLauncher;

    // tag::readerwriterprocessor[]
    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{jobParameters['date']}") String s) {
        System.out.print("s:" + s);
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"firstName", "lastName"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer() {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource);
        return writer;
    }
    // end::readerwriterprocessor[]

    // tag::listener[]

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionNotificationListener(new JdbcTemplate(dataSource));
    }

    // end::listener[]

    // tag::jobstep[]
    @Bean
    public Job importUserJob2(FlatFileItemReader<Person> ir, @Qualifier("step2") Step step2) {
        return jobBuilderFactory.get("importUserJob2")
                .incrementer(new RunIdIncrementer())
                .listener(new JobExecutionListener() {

                    @Override
                    public void beforeJob(JobExecution jobExecution) {

                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        System.out.println(jobExecution);

                        if (jobExecution.getStatus() == BatchStatus.FAILED) {
                            throw new Error("");
                            //throw new RuntimeException("");
                        }

                    }
                })
                .flow(step2)
                .end()
                .build();
    }


    @Autowired
    @Qualifier("step1")
    private Step step;

    @Scheduled(cron = "0 0 0 * * *")
    public void perform() {
        try {
            jobLauncher.run(jobBuilderFactory.get("importUserJob")
                    .incrementer(new RunIdIncrementer())
                    .listener(listener())
                    .flow(step)
                    .end()
                    .build(), new JobParametersBuilder().addString("date", (LocalDateTime.now()).toString()).toJobParameters());
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }

    }





    @Bean
    public Step step1(FlatFileItemReader<Person> ir) {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(ir)
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public Step step2(FlatFileItemReader<Person> ir) {
        return stepBuilderFactory.get("step2")
                .<Person, Person>chunk(10)
                .reader(ir)
//                .writer((map -> {
//                    throw new SQLException("test");
//                }))

                .writer(writer())
                .build();
    }

    @Autowired
    private SmokeImportUserJobHealthCheck smokeImportUserJobHealthCheck;


    @Override
    public void run(String... strings) throws Exception {
        try {

            final JobExecution jobExecution = jobLauncher.run(jobBuilderFactory.get("importUserJob")
                    .incrementer(new RunIdIncrementer())
                    .listener(listener())
                    .flow(step)
                    .end()
                    .build(), new JobParametersBuilder().addString("date", (LocalDateTime.now()).toString()).toJobParameters());
            final BatchStatus batchStatus = jobExecution.getStatus();
            log.info("batchStatus:{} before sleep", batchStatus);
            Thread.sleep(10000);

            smokeImportUserJobHealthCheck.setBatchStatus(batchStatus);
            log.info("batchStatus:{}", batchStatus);
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }


    }

    @Bean
    public SmokeImportUserJobHealthCheck smokeImportUserJobHealthCheck() {
        return new SmokeImportUserJobHealthCheck();
    }


// end::jobstep[]
}

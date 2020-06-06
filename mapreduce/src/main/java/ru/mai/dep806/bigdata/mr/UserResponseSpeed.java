package ru.mai.dep806.bigdata.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UserResponseSpeed extends Configured implements Tool {
    private static final char FIELD_SEPARATOR = '\0';
    private static Log log = LogFactory.getLog(UserResponseSpeed.class);

    static final String[] QUESTION_FIELDS = new String[]{
            "AcceptedAnswerId", "CreationDate" // AcceptedAnswerId - KEY
    };

    static final String[] ANSWER_FIELDS = new String[]{
            "Id", "CreationDate", "OwnerUserId" // Id - KEY
    };

    static final String[] POST_FIELDS = new String[]{
            "QuestionCreationDate", "AnswerCreationDate", "OwnerUserId"
    };

    static final String[] POST_MAP_FIELDS = new String[]{
            "TimeDelta"
    };

    static final String[] USER_FIELDS = new String[]{
            "Id", "DisplayName"
    };

    static final String[] RESULT_FIELDS = new String[]{
            "UserId", "DisplayName", "AnswersCount"
    };

    static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    static String concatenateFields(StringBuilder buffer, Map<String, String> row, char type, String[] fields) {
        buffer.setLength(0);
        if (type > 0) {
            buffer.append(type).append(FIELD_SEPARATOR); // Для обозначения строки типа User или Post
        }
        for (String field : fields) {
            String fieldValue = row.get(field);
            if (fieldValue != null) {
                buffer.append(fieldValue);
            }
            buffer.append(FIELD_SEPARATOR);
        }
        return buffer.toString();
    }

    static Map<String, String> parseFields(String buffer, String[] fields) {
        String[] values = StringUtils.split(buffer.substring(2, buffer.length() - 1), FIELD_SEPARATOR); // Удаляем первые 2 символа - тип строки и разделитель
        return IntStream.range(0, values.length).boxed()
                .collect(Collectors.toMap(i -> fields[i], i -> values[i]));
    }

    /***************
     *   STAGE 1   *
     ***************/

    private static class PostsQuestionAnswerMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        private final StringBuilder buffer = new StringBuilder();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
//            log.info("map keys = " + String.join(", ", row.keySet()));

            if (row.get("PostTypeId") != null) {
                if (row.get("PostTypeId").equals("1")) {
                    String keyQuestionString = row.get("AcceptedAnswerId");

                    if (StringUtils.isNotBlank(keyQuestionString)) {
                        outKey.set(Long.parseLong(keyQuestionString));
                        outValue.set(concatenateFields(buffer, row, 'Q', QUESTION_FIELDS));
                        context.write(outKey, outValue);
                    }
                } else if (row.get("PostTypeId").equals("2")) {
                    String keyAnswerString = row.get("Id");

                    if (StringUtils.isNotBlank(keyAnswerString)) {
                        outKey.set(Long.parseLong(keyAnswerString));
                        outValue.set(concatenateFields(buffer, row, 'A', ANSWER_FIELDS));
                        context.write(outKey, outValue);
                    }
                }
            }
        }

    }


    static class JoinQuestionAnswerReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> questions = new ArrayList<>();
            List<String> answers = new ArrayList<>();

            // Распределим значения по типам строк в соотв. списки
            for (Text value : values) {
                String strValue = value.toString();
                switch (strValue.charAt(0)) {
                    case 'Q':
                        questions.add(strValue);
                        break;
                    case 'A':
                        answers.add(strValue);
                        break;
                    default:
                        log.error("Unknown type: " + strValue.charAt(0));
                        throw new IllegalStateException("Unknown type: " + strValue.charAt(0));
                }
            }

//            log.info("questions.size = " + questions.size() + "answers.size = " + answers.size());
            // Если с обеих сторон есть строки для данного ключа (inner join)
            if (questions.size() > 0 && answers.size() > 0) {
                // Выполним Join
                for (String question : questions) {
                    for (String answer : answers) {
                        Map<String, String> question_row = parseFields(question, QUESTION_FIELDS);
                        Map<String, String> answer_row = parseFields(answer, ANSWER_FIELDS);

                        String questionTimeStr = question_row.get("CreationDate");
                        String answerTimeStr = answer_row.get("CreationDate");
                        try {
                            long timeDelta = (DATE_FORMAT.parse(answerTimeStr).getTime() -
                                    DATE_FORMAT.parse(questionTimeStr).getTime());
                            if (timeDelta > 0.3 * 1000 * 60) { // where (a_creationdate - q_creationdate) / 60 > 0.3
//                            log.info("OwnerUserId = " + answer_row.get("OwnerUserId"));
                                String userId = answer_row.get("OwnerUserId");
                                if (userId != null) {
                                    outKey.set(Long.parseLong(userId));
                                    outValue.set("P" + FIELD_SEPARATOR + timeDelta + FIELD_SEPARATOR);
                                    context.write(outKey, outValue);
                                }
                            }
                        } catch (ParseException e) {
                            log.info("Error while date parsing");
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    /***************
     *   STAGE 2   *
     ***************/

    private static class UserMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        private final StringBuilder buffer = new StringBuilder();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            String keyString = row.get("Id");

            if (StringUtils.isNotBlank(keyString)) {
                outKey.set(Long.parseLong(keyString));
                outValue.set(concatenateFields(buffer, row, 'U', USER_FIELDS));
                context.write(outKey, outValue);
            }
        }
    }


    static class JoinPostUserReducer extends Reducer<LongWritable, Text, DoubleWritable, Text> {

        private final DoubleWritable outKey = new DoubleWritable();
        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> posts = new ArrayList<>();
            List<String> users = new ArrayList<>();

            // Распределим значения по типам строк в соотв. списки
            for (Text value : values) {
                String strValue = value.toString();
                switch (strValue.charAt(0)) {
                    case 'P':
                        posts.add(strValue);
                        break;
                    case 'U':
                        users.add(strValue);
                        break;
                    default:
                        log.error("Unknown type: " + strValue.charAt(0));
                        throw new IllegalStateException("Unknown type: " + strValue.charAt(0));
                }
            }

            // Если с обеих сторон есть строки для данного ключа (inner join)
            if (posts.size() * users.size() > 1) { // having count(*) > 1
                double answerTime = 0;
                String displayName = "";
                int rowCount = posts.size() * users.size();
                // Выполним Join
                for (String post : posts) {
                    for (String user : users) {
                        Map<String, String> postRow = parseFields(post, POST_MAP_FIELDS);
                        Map<String, String> userRow = parseFields(user, USER_FIELDS);
                        displayName = userRow.get("DisplayName");
//                        long timeDelta = Long.parseLong(postRow.get("TimeDelta"));
//                        answerTime += timeDelta;
                        answerTime += Long.parseLong(postRow.get("TimeDelta"));
                    }
                }
//                static final String[] RESULT_FIELDS = new String[]{
//                        "Id", "DisplayName", "MeanTimeDelta", "AnswersCount"
//                };
                Map<String, String> row = new HashMap<>();
                row.put("UserId", key.toString());
                row.put("DisplayName", displayName);
                row.put("AnswersCount", String.valueOf(rowCount));

                outKey.set(answerTime / (1000 * 60 * rowCount));
                outValue.set(concatenateFields(buffer, row, (char) 0, RESULT_FIELDS));
//                outValue.set(String.join("\t", new String[]{displayName,
//                                String.valueOf(answerTime / (1000 * 60 * rowCount)), String.valueOf(rowCount)}));
//                        concatenateFields(buffer, row, (char) 0, RESULT_FIELDS));
                context.write(outKey, outValue);
            }
        }
    }

    /***************
     *   STAGE 3   *
     ***************/

    static class EmptyMapper extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        protected void map(DoubleWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }



    static class SortReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        private final Text outValue = new Text();
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Map<String, String> postRow = parseFields(value.toString(), RESULT_FIELDS);
                outValue.set(String.join("\t", new String[]{postRow.get("UserId"),
                        postRow.get("DisplayName"), postRow.get("AnswersCount")}));
                context.write(key, outValue);

            }
        }
    }

    /*
    f (questions.size() > 1 && answers.size() > 1) { // having count(*) > 1
                double answerTime = 0;
                int rowCount = questions.size() * answers.size();
                // Выполним Join
                for (String question : questions) {
                    for (String answer : answers) {
                        Map<String, String> questionMap = parseFields(question, QUESTION_FIELDS);
                        Map<String, String> answerMap = parseFields(answer, ANSWER_FIELDS);
                        String questionTimeStr = questionMap.get("CreationDate");
                        String answerTimeStr = answerMap.get("CreationDate");
                        if (StringUtils.isNotBlank(questionTimeStr) && StringUtils.isNotBlank(answerTimeStr)) {
                            try {
                                double dateDiff = (DATE_FORMAT.parse(questionTimeStr).getTime() -
                                        DATE_FORMAT.parse(answerTimeStr).getTime())/ 60.0;
                                if (dateDiff > 0.3) {
                                    answerTime += dateDiff;
                                }
                            } catch (ParseException e) {
                                throw new IllegalArgumentException("Error while parsing date " + questionTimeStr +
                                        " or " + answerTimeStr);
                            }
                        }
                    }
                }
                outValue.set(String.valueOf(answerTime / rowCount) + FIELD_SEPARATOR
                        + rowCount + FIELD_SEPARATOR);
                context.write(key, outValue);
     */


    @Override
    public int run(String[] args) throws Exception {
        Path postsPath = new Path(args[0]);
        Path usersPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Path stagingPath = new Path(outputPath + "-stage-1");
        Path stagingPath2 = new Path(outputPath + "-stage-2");

        FileSystem.get(new Configuration()).delete(outputPath, true);

//         Создаем новую задачу (Job), указывая ее название
        Job QAJoinJob = Job.getInstance(getConf(), "Vatolin Join Posts question and answers");
        // Указываем архив с задачей по имени класса в этом архиве
        QAJoinJob.setJarByClass(UserResponseSpeed.class);

        QAJoinJob.setMapperClass(PostsQuestionAnswerMapper.class);
        // Указываем класс Редьюсера
        QAJoinJob.setReducerClass(JoinQuestionAnswerReducer.class);
        // Кол-во тасков
        QAJoinJob.setNumReduceTasks(10);
        // Тип ключа на выходе
        QAJoinJob.setOutputKeyClass(LongWritable.class);
        // Тип значения на выходе
        QAJoinJob.setOutputValueClass(Text.class);
        // Пути к входным файлам, формат файла и мэппер
        FileInputFormat.addInputPath(QAJoinJob, postsPath);
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(QAJoinJob, stagingPath);
        QAJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Включаем компрессию
        FileOutputFormat.setCompressOutput(QAJoinJob, true);
        FileOutputFormat.setOutputCompressorClass(QAJoinJob, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(QAJoinJob, SequenceFile.CompressionType.BLOCK);

//         Запускаем джобу и ждем окончания ее выполнения
        boolean success = QAJoinJob.waitForCompletion(true);
        if (success) {
            // Создаем новую задачу (Job), указывая ее название
            Job UserJoinJob = Job.getInstance(getConf(), "Vatolin Join Posts and Users");
            // Указываем архив с задачей по имени класса в этом архиве
            UserJoinJob.setJarByClass(UserResponseSpeed.class);
            // Указываем класс Редьюсера
            UserJoinJob.setReducerClass(JoinPostUserReducer.class);
            // Кол-во тасков
            UserJoinJob.setNumReduceTasks(10);

            UserJoinJob.setMapOutputKeyClass(LongWritable.class);
            UserJoinJob.setMapOutputValueClass(Text.class);

            // Тип ключа на выходе
            UserJoinJob.setOutputKeyClass(DoubleWritable.class);
            UserJoinJob.setOutputValueClass(Text.class);
            // Пути к входным файлам, формат файла и мэппер
            MultipleInputs.addInputPath(UserJoinJob, stagingPath, SequenceFileInputFormat.class, Mapper.class);
            MultipleInputs.addInputPath(UserJoinJob, usersPath, TextInputFormat.class, UserMapper.class);
            // Путь к файлу на выход (куда запишутся результаты)
            FileOutputFormat.setOutputPath(UserJoinJob, stagingPath2);
            UserJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            // Включаем компрессию
            FileOutputFormat.setCompressOutput(UserJoinJob, true);
            FileOutputFormat.setOutputCompressorClass(UserJoinJob, SnappyCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(UserJoinJob, SequenceFile.CompressionType.BLOCK);

            success = UserJoinJob.waitForCompletion(true);

            if (success) {
                Job sortJob = Job.getInstance(getConf(), "Vatolin Sort");
                sortJob.setJarByClass(UserResponseSpeed.class);
                sortJob.setMapperClass(EmptyMapper.class);
                sortJob.setReducerClass(SortReducer.class);
                sortJob.setNumReduceTasks(1);

                sortJob.setOutputKeyClass(DoubleWritable.class);
                sortJob.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(sortJob, stagingPath2);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);

                FileOutputFormat.setOutputPath(sortJob, outputPath);
                sortJob.setOutputFormatClass(TextOutputFormat.class);

                success = sortJob.waitForCompletion(true);
            }
        }
//        FileSystem.get(new Configuration()).delete(stagingPath, true);

        return success ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        // yarn jar mr-jobs.jar ru.mai.dep806.bigdata.mr.UserResponseSpeed /user/stud/stackoverflow/landing/Posts_sample /user/stud/stackoverflow/landing/Users /user/stud/vatolin/post_user_join_sample
        int result = ToolRunner.run(new Configuration(), new UserResponseSpeed(), args);

        System.exit(result);
    }
}

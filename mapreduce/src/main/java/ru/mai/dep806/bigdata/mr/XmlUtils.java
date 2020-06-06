package ru.mai.dep806.bigdata.mr;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Simple xml parsing utilities.
 */
public class XmlUtils {

    // Example row:
    //  <row Id="8" Reputation="947" CreationDate="2008-07-31T21:33:24.057" DisplayName="Eggs McLaren" LastAccessDate="2012-10-15T22:00:45.510" WebsiteUrl="" Location="" AboutMe="&lt;p&gt;This is a puppet test account." Views="5163" UpVotes="12" DownVotes="9" AccountId="6" />
    public static Map<String, String> parseXmlRow(String xml) {
        Map<String, String> map = new HashMap<>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }
    static final String FIELD_SEPARATOR = "\0";

    static final String[] ANSWER_FIELDS = new String[]{
            "Id", "CreationDate", "OwnerUserId" // Id - KEY
    };

    static Map<String, String> parseFields(String buffer, String[] fields) {
        String[] values = buffer.substring(2).split(FIELD_SEPARATOR); // Удаляем первые 2 символа - тип строки и разделитель
        return IntStream.range(0, values.length).boxed()
                .collect(Collectors.toMap(i -> fields[i], i -> values[i]));
    }


    public static void main(String[] args) {
        String s = "<row Id=\"6\" PostTypeId=\"1\" AcceptedAnswerId=\"31\" CreationDate=\"2008-07-31T22:08:08.620\" Score=\"290\" ViewCount=\"18713\"\n" +
                "     Body=\"&lt;p&gt;I have an absolutely positioned &lt;code&gt;div&lt;/code&gt; containing several children, one of which is a relatively positioned &lt;code&gt;div&lt;/code&gt;. When I use a &lt;code&gt;percentage-based width&lt;/code&gt; on the child &lt;code&gt;div&lt;/code&gt;, it collapses to &lt;code&gt;0 width&lt;/code&gt; on IE7, but not on Firefox or Safari.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;If I use &lt;code&gt;pixel width&lt;/code&gt;, it works. If the parent is relatively positioned, the percentage width on the child works.&lt;/p&gt;&#xA;&#xA;&lt;ol&gt;&#xA;&lt;li&gt;Is there something I'm missing here?&lt;/li&gt;&#xA;&lt;li&gt;Is there an easy fix for this besides the &lt;code&gt;pixel-based width&lt;/code&gt; on the&#xA;child?&lt;/li&gt;&#xA;&lt;li&gt;Is there an area of the CSS specification that covers this?&lt;/li&gt;&#xA;&lt;/ol&gt;&#xA;\"\n" +
                "     OwnerUserId=\"9\" LastEditorUserId=\"3641067\" LastEditorDisplayName=\"Rich B\" LastEditDate=\"2019-07-19T01:43:04.077\"\n" +
                "     LastActivityDate=\"2019-07-19T01:43:04.077\"\n" +
                "     Title=\"Percentage width child element in absolutely positioned parent on Internet Explorer 7\"\n" +
                "     Tags=\"&lt;html&gt;&lt;css&gt;&lt;internet-explorer-7&gt;\" AnswerCount=\"6\" CommentCount=\"0\" FavoriteCount=\"11\"/>";
        Map<String, String> res = parseXmlRow(s);
        char t = (char) 0;

//        String row_s = "A" + '\0' + "210" + '\0' + "2008-08-01T20:02:08.357" + '\0' + "39" + '\0';
        String row_s = "A267" + '\0' + "2008-08-05T18:36:01.227" + '\0' + "383";
        Map<String, String> parsed_row = parseFields(row_s, ANSWER_FIELDS);

        DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        String questionTimeStr = "2008-08-01T20:02:08.357";
        String answerTimeStr = "2008-08-01T20:02:09.357";
        try {
            Date d = DATE_FORMAT.parse(parsed_row.get("CreationDate"));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        int i = 0;
    }
}

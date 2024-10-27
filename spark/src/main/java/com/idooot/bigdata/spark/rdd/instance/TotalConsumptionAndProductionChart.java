package com.idooot.bigdata.spark.rdd.instance;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Stream;
public class TotalConsumptionAndProductionChart {
    public static void main(String[] args) throws IOException {
        // 创建时间序列对象
        TimeSeries consumptionSeries = new TimeSeries("Total Consumption");
        TimeSeries productionSeries = new TimeSeries("Total Production");

        // 查找并读取所有 part-* 文件
        try (Stream<Path> paths = Files.list(Paths.get("data/output/totalConsumptionAndProduction"))) {
            paths.filter(path -> path.getFileName().toString().startsWith("part-")).forEach(path -> {
                try {
                    // 读取每个文件的内容并解析
                    List<String> lines = Files.readAllLines(path);
                    for (String line : lines) {
                        line = line.replaceAll("[()]", "");
                        String[] parts = line.split(",");
                        String dateTime = parts[0]; // 提取日期时间
                        double consumption = Double.parseDouble(parts[1]);
                        double production = Double.parseDouble(parts[2]);

                        // 将日期时间解析为 Minute 格式
                        String[] dateTimeParts = dateTime.split(" ");
                        String date = dateTimeParts[0];
                        String time = dateTimeParts[1];
                        String[] dateParts = date.split("-");
                        String[] timeParts = time.split(":");

                        int year = Integer.parseInt(dateParts[0]);
                        int month = Integer.parseInt(dateParts[1]);
                        int day = Integer.parseInt(dateParts[2]);
                        int hour = Integer.parseInt(timeParts[0]);
                        int minute = Integer.parseInt(timeParts[1]);

                        Minute minuteData = new Minute(minute, hour, day, month, year);
                        consumptionSeries.addOrUpdate(minuteData, consumption);
                        productionSeries.addOrUpdate(minuteData, production);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + path);
                    e.printStackTrace();
                }
            });
        }

        // 创建数据集
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(consumptionSeries);
        dataset.addSeries(productionSeries);

        // 创建图表
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Total Consumption and Production Trends",  // 图表标题
                "DateTime",                                // X轴标签
                "Values",                                  // Y轴标签
                dataset,
                true,                                      // 显示图例
                true,                                      // 显示工具提示
                false                                      // 不使用URL链接
        );

        // 设置字体以避免乱码
        chart.getTitle().setFont(new Font("Arial", Font.BOLD, 18));
        chart.getLegend().setItemFont(new Font("Arial", Font.PLAIN, 12));
        XYPlot plot = chart.getXYPlot();
        plot.getDomainAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));
        plot.getRangeAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));

        // 设置日期格式化
        DateAxis dateAxis = (DateAxis) plot.getDomainAxis();
        dateAxis.setDateFormatOverride(new SimpleDateFormat("yyyy-MM-dd HH:mm"));

        // 保存图表到指定路径
        ChartUtils.saveChartAsPNG(new File("data/output/TotalConsumptionAndProductionChart.png"), chart, 1200, 800);
        System.out.println("Chart saved as data/output/TotalConsumptionAndProductionChart.png");
    }
}

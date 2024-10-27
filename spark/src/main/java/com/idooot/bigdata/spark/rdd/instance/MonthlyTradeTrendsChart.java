package com.idooot.bigdata.spark.rdd.instance;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.Plot;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.title.LegendTitle;
import org.jfree.chart.ui.RectangleEdge;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.chart.axis.ValueAxis;

import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Stream;


public class MonthlyTradeTrendsChart {

    public static void main(String[] args) throws IOException {
        // 创建时间序列对象
        TimeSeries quantitySeries = new TimeSeries("Total Quantity");
        TimeSeries valueSeries = new TimeSeries("Total Value");

        // 查找并读取所有 part-* 文件
        try (Stream<Path> paths = Files.list(Paths.get("data/output/monthlyTradeTrends"))) {
            paths.filter(path -> path.getFileName().toString().startsWith("part-")).forEach(path -> {
                try {
                    // 读取每个文件的内容并解析
                    List<String> lines = Files.readAllLines(path);
                    for (String line : lines) {
                        line = line.replaceAll("[()]", "");
                        String[] parts = line.split(",");
                        String date = parts[0].split("-")[0]; // 提取年份
                        int month = Integer.parseInt(parts[0].split("-")[1]); // 提取月份

                        double quantity = Double.parseDouble(parts[1]);
                        double value = Double.parseDouble(parts[2]);

                        quantitySeries.addOrUpdate(new Month(month, Integer.parseInt(date)), quantity);
                        valueSeries.addOrUpdate(new Month(month, Integer.parseInt(date)), value);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + path);
                    e.printStackTrace();
                }
            });
        }

        // 创建数据集
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(quantitySeries);
        dataset.addSeries(valueSeries);

        // 创建图表
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Monthly Trade Trends",  // 图表标题
                "Date",                 // X轴标签
                "Values",               // Y轴标签
                dataset,
                true,                   // 显示图例
                true,                   // 显示工具提示
                false                   // 不使用URL链接
        );

        // 设置字体以避免乱码
        chart.getTitle().setFont(new Font("Arial", Font.BOLD, 18));
        chart.getLegend().setItemFont(new Font("Arial", Font.PLAIN, 12));

        // 获取 XYPlot 并设置自定义日期轴格式
        XYPlot plot = chart.getXYPlot();
        plot.getDomainAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));
        plot.getRangeAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));

        // 设置日期格式化
        DateAxis dateAxis = (DateAxis) plot.getDomainAxis();
        dateAxis.setDateFormatOverride(new SimpleDateFormat("yyyy-MM"));

        // 保存图表到指定路径
        ChartUtils.saveChartAsPNG(new File("data/output/MonthlyTradeTrendsChart.png"), chart, 800, 600);
        System.out.println("Chart saved as data/output/MonthlyTradeTrendsChart.png");
    }
}

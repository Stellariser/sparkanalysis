package com.idooot.bigdata.spark.rdd.instance;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;



public class MainTradePartnersChart {
    public static void main(String[] args) {
        // 创建数据集
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // 统计每个贸易伙伴的出现频率
        Map<String, Integer> partnerFrequency = new HashMap<>();

        // 读取数据
        Path dataPath = Paths.get("data/output/mainTradePartners");
        if (!Files.exists(dataPath)) {
            System.err.println("Data directory does not exist: " + dataPath);
            return;
        }

        // 读取所有 part-* 文件
        try (Stream<Path> paths = Files.list(dataPath)) {
            paths.filter(path -> path.getFileName().toString().startsWith("part-")).forEach(path -> {
                try {
                    List<String> lines = Files.readAllLines(path);
                    for (String line : lines) {
                        line = line.replaceAll("[()\\[\\]]", "").trim();
                        String[] parts = line.split(",");
                        if (parts.length < 2) continue;

                        for (int i = 1; i < parts.length; i++) {
                            String partner = parts[i].trim();
                            partnerFrequency.put(partner, partnerFrequency.getOrDefault(partner, 0) + 1);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + path);
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            System.err.println("Error listing files in directory: " + dataPath);
            e.printStackTrace();
            return;
        }
        partnerFrequency.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())  // 按值降序排序
                .forEach(entry -> dataset.addValue(entry.getValue(), "Trade Partners", entry.getKey()));


        // 将数据添加到数据集
        partnerFrequency.forEach((partner, frequency) -> dataset.addValue(frequency, "Trade Partners", partner));

        // 创建条形图
        JFreeChart barChart = ChartFactory.createBarChart(
                "Main Trade Partners Frequency",
                "Trade Partner",
                "Frequency",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        // 设置字体
        barChart.getTitle().setFont(new Font("Arial", Font.BOLD, 18));
        barChart.getCategoryPlot().getDomainAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));
        barChart.getCategoryPlot().getRangeAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));

        // 保存图表
        File outputFile = new File("data/output/MainTradePartnersChart.png");
        try {
            ChartUtils.saveChartAsPNG(outputFile, barChart, 10000, 800);
            System.out.println("Chart saved as " + outputFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error saving chart as PNG: " + outputFile);
            e.printStackTrace();
        }
    }
}

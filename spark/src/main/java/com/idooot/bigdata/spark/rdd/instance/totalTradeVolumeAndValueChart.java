package com.idooot.bigdata.spark.rdd.instance;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.LogAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYDotRenderer;
import org.jfree.data.xy.DefaultXYDataset;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class totalTradeVolumeAndValueChart {
    public static void main(String[] args) {
        // 创建数据集
        DefaultXYDataset dataset = new DefaultXYDataset();

        // 数据路径
        Path dataPath = Paths.get("data/output/totalTradeVolumeAndValue");
        if (!Files.exists(dataPath)) {
            System.err.println("Data directory does not exist: " + dataPath);
            return;
        }

        // 读取数据并添加到列表
        List<double[]> dataList = new ArrayList<>();
        try (Stream<Path> paths = Files.list(dataPath)) {
            paths.filter(path -> path.getFileName().toString().startsWith("part-")).forEach(path -> {
                try {
                    List<String> lines = Files.readAllLines(path);
                    for (String line : lines) {
                        line = line.replaceAll("[()]", "").trim();
                        String[] parts = line.split(",");
                        if (parts.length < 3) continue;

                        double volume = Double.parseDouble(parts[1]);
                        double value = Double.parseDouble(parts[2]);
                        dataList.add(new double[]{volume, value});
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

        // 将列表数据转换为数组并添加到数据集中
        if (!dataList.isEmpty()) {
            double[][] data = new double[2][dataList.size()];
            for (int i = 0; i < dataList.size(); i++) {
                data[0][i] = dataList.get(i)[0];  // 交易量 (x)
                data[1][i] = dataList.get(i)[1];  // 交易额 (y)
            }
            dataset.addSeries("Trade Data", data);
        }

        // 创建散点图
        JFreeChart scatterChart = ChartFactory.createScatterPlot(
                "Trade Volume vs. Trade Value",
                "Trade Volume", "Trade Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        // 定制化图表外观
        XYPlot plot = scatterChart.getXYPlot();

        // 设置对数刻度以平衡数据显示
        plot.setDomainAxis(new LogAxis("Trade Volume (Log Scale)"));
        plot.setRangeAxis(new LogAxis("Trade Value (Log Scale)"));

        // 调整点的大小
        XYDotRenderer renderer = new XYDotRenderer();
        renderer.setDotWidth(3); // 减小点的宽度
        renderer.setDotHeight(3); // 减小点的高度
        plot.setRenderer(renderer);

        // 设置字体
        scatterChart.getTitle().setFont(new Font("Arial", Font.BOLD, 18));
        plot.getDomainAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));
        plot.getRangeAxis().setLabelFont(new Font("Arial", Font.PLAIN, 14));

        // 保存图表
        File outputFile = new File("data/output/TradeVolumeValueScatterChart.png");
        try {
            ChartUtils.saveChartAsPNG(outputFile, scatterChart, 1200, 800); // 增加图表尺寸
            System.out.println("Scatter chart saved as " + outputFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error saving chart as PNG: " + outputFile);
            e.printStackTrace();
        }
    }
}

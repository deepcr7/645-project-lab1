package buffermanager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import javax.imageio.ImageIO;

public class PlotGenerator {

    private static final int WIDTH = 800;
    private static final int HEIGHT = 600;
    private static final int MARGIN = 70;

    public static void main(String[] args) {
        try {
            generateP1Plots();
            generateP2Plots();
            generateP3Plots();
            System.out.println("All plots generated successfully.");
        } catch (IOException e) {
            System.err.println("Error generating plots: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void generateP1Plots() throws IOException {
        List<Double> selectivity = new ArrayList<>();
        List<Double> directScanTime = new ArrayList<>();
        List<Double> indexScanTime = new ArrayList<>();
        List<Double> ratio = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader("p1_results.csv"))) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                selectivity.add(Double.parseDouble(parts[0]));
                directScanTime.add(Double.parseDouble(parts[1]));
                indexScanTime.add(Double.parseDouble(parts[2]));
                double r = (Double.parseDouble(parts[2]) > 0)
                        ? Double.parseDouble(parts[1]) / Double.parseDouble(parts[2])
                        : 0;
                ratio.add(r);
            }
        }
        BufferedImage image1 = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g1 = image1.createGraphics();
        drawPlot(g1, selectivity, directScanTime, indexScanTime,
                "Query Execution Time vs. Selectivity (Title Index)",
                "Selectivity", "Execution Time (ms)",
                "Direct Scan", "Index Scan");
        ImageIO.write(image1, "png", new File("p1_execution_time.png"));
        BufferedImage image2 = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2 = image2.createGraphics();
        drawRatioPlot(g2, selectivity, ratio,
                "Speed Ratio vs. Selectivity (Title Index)",
                "Selectivity", "Speed Ratio (Direct/Index)");
        ImageIO.write(image2, "png", new File("p1_ratio.png"));
    }

    private static void generateP2Plots() throws IOException {
        List<Double> selectivity = new ArrayList<>();
        List<Double> directScanTime = new ArrayList<>();
        List<Double> indexScanTime = new ArrayList<>();
        List<Double> ratio = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader("p2_results.csv"))) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                selectivity.add(Double.parseDouble(parts[0]));
                directScanTime.add(Double.parseDouble(parts[1]));
                indexScanTime.add(Double.parseDouble(parts[2]));
                double r = (Double.parseDouble(parts[2]) > 0)
                        ? Double.parseDouble(parts[1]) / Double.parseDouble(parts[2])
                        : 0;
                ratio.add(r);
            }
        }
        BufferedImage image1 = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g1 = image1.createGraphics();
        drawPlot(g1, selectivity, directScanTime, indexScanTime,
                "Query Execution Time vs. Selectivity (MovieId Index)",
                "Selectivity", "Execution Time (ms)",
                "Direct Scan", "Index Scan");
        ImageIO.write(image1, "png", new File("p2_execution_time.png"));
        BufferedImage image2 = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2 = image2.createGraphics();
        drawRatioPlot(g2, selectivity, ratio,
                "Speed Ratio vs. Selectivity (MovieId Index)",
                "Selectivity", "Speed Ratio (Direct/Index)");
        ImageIO.write(image2, "png", new File("p2_ratio.png"));
    }

    private static void generateP3Plots() throws IOException {
        List<Double> selectivity = new ArrayList<>();
        List<Double> titleIndexTime = new ArrayList<>();
        List<Double> movieIdIndexTime = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader("p3_results.csv"))) {
            String line = reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                selectivity.add(Double.parseDouble(parts[0]));
                titleIndexTime.add(Double.parseDouble(parts[1]));
                movieIdIndexTime.add(Double.parseDouble(parts[2]));
            }
        }

        // Create execution time plot
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = image.createGraphics();
        drawPlot(g, selectivity, titleIndexTime, movieIdIndexTime,
                "Query Execution Time with Pinned First Levels",
                "Selectivity", "Execution Time (ms)",
                "Title Index", "MovieId Index");
        ImageIO.write(image, "png", new File("p3_pinned_levels.png"));
    }

    private static void drawPlot(Graphics2D g, List<Double> x, List<Double> y1, List<Double> y2,
            String title, String xLabel, String yLabel,
            String series1Name, String series2Name) {
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, WIDTH, HEIGHT);
        double maxX = 0;
        double maxY = 0;
        for (int i = 0; i < x.size(); i++) {
            maxX = Math.max(maxX, x.get(i));
            maxY = Math.max(maxY, Math.max(y1.get(i), y2.get(i)));
        }
        maxY = maxY * 1.1;
        g.setColor(Color.BLACK);
        g.setStroke(new BasicStroke(2));
        g.drawLine(MARGIN, HEIGHT - MARGIN, WIDTH - MARGIN, HEIGHT - MARGIN);
        g.drawLine(MARGIN, HEIGHT - MARGIN, MARGIN, MARGIN);
        g.setFont(new Font("Arial", Font.BOLD, 20));
        int titleWidth = g.getFontMetrics().stringWidth(title);
        g.drawString(title, (WIDTH - titleWidth) / 2, 30);

        g.setFont(new Font("Arial", Font.PLAIN, 14));
        int xLabelWidth = g.getFontMetrics().stringWidth(xLabel);
        g.drawString(xLabel, (WIDTH - xLabelWidth) / 2, HEIGHT - 20);

        g.rotate(-Math.PI / 2);
        g.drawString(yLabel, -HEIGHT / 2 - g.getFontMetrics().stringWidth(yLabel) / 2, 20);
        g.rotate(Math.PI / 2);
        for (int i = 0; i <= 5; i++) {
            double value = maxX * i / 5.0;
            String label = String.format("%.2f", value);
            int labelWidth = g.getFontMetrics().stringWidth(label);
            int x_pos = MARGIN + (WIDTH - 2 * MARGIN) * i / 5;
            g.drawString(label, x_pos - labelWidth / 2, HEIGHT - MARGIN + 20);
            g.drawLine(x_pos, HEIGHT - MARGIN, x_pos, HEIGHT - MARGIN + 5);
        }
        for (int i = 0; i <= 5; i++) {
            double value = maxY * i / 5.0;
            String label = String.format("%.1f", value);
            int y_pos = HEIGHT - MARGIN - (HEIGHT - 2 * MARGIN) * i / 5;
            g.drawString(label, MARGIN - 10 - g.getFontMetrics().stringWidth(label), y_pos + 5);
            g.drawLine(MARGIN - 5, y_pos, MARGIN, y_pos);
        }
        g.setColor(Color.BLUE);
        g.setStroke(new BasicStroke(2));
        for (int i = 0; i < x.size() - 1; i++) {
            int x1_pos = MARGIN + (int) ((x.get(i) / maxX) * (WIDTH - 2 * MARGIN));
            int y1_pos = HEIGHT - MARGIN - (int) ((y1.get(i) / maxY) * (HEIGHT - 2 * MARGIN));
            int x2_pos = MARGIN + (int) ((x.get(i + 1) / maxX) * (WIDTH - 2 * MARGIN));
            int y2_pos = HEIGHT - MARGIN - (int) ((y1.get(i + 1) / maxY) * (HEIGHT - 2 * MARGIN));
            g.drawLine(x1_pos, y1_pos, x2_pos, y2_pos);
        }
        g.setColor(Color.RED);
        for (int i = 0; i < x.size() - 1; i++) {
            int x1_pos = MARGIN + (int) ((x.get(i) / maxX) * (WIDTH - 2 * MARGIN));
            int y1_pos = HEIGHT - MARGIN - (int) ((y2.get(i) / maxY) * (HEIGHT - 2 * MARGIN));
            int x2_pos = MARGIN + (int) ((x.get(i + 1) / maxX) * (WIDTH - 2 * MARGIN));
            int y2_pos = HEIGHT - MARGIN - (int) ((y2.get(i + 1) / maxY) * (HEIGHT - 2 * MARGIN));
            g.drawLine(x1_pos, y1_pos, x2_pos, y2_pos);
        }
        g.setColor(Color.WHITE);
        g.fillRect(WIDTH - 200, 50, 180, 60);
        g.setColor(Color.BLACK);
        g.drawRect(WIDTH - 200, 50, 180, 60);

        g.setColor(Color.BLUE);
        g.drawLine(WIDTH - 180, 70, WIDTH - 140, 70);
        g.setColor(Color.BLACK);
        g.drawString(series1Name, WIDTH - 130, 75);

        g.setColor(Color.RED);
        g.drawLine(WIDTH - 180, 100, WIDTH - 140, 100);
        g.setColor(Color.BLACK);
        g.drawString(series2Name, WIDTH - 130, 105);
    }

    private static void drawRatioPlot(Graphics2D g, List<Double> x, List<Double> y,
            String title, String xLabel, String yLabel) {
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, WIDTH, HEIGHT);
        double maxX = 0;
        double maxY = 0;
        for (int i = 0; i < x.size(); i++) {
            maxX = Math.max(maxX, x.get(i));
            maxY = Math.max(maxY, y.get(i));
        }
        maxY = maxY * 1.1;
        g.setColor(Color.BLACK);
        g.setStroke(new BasicStroke(2));
        g.drawLine(MARGIN, HEIGHT - MARGIN, WIDTH - MARGIN, HEIGHT - MARGIN);
        g.drawLine(MARGIN, HEIGHT - MARGIN, MARGIN, MARGIN);
        g.setFont(new Font("Arial", Font.BOLD, 20));
        int titleWidth = g.getFontMetrics().stringWidth(title);
        g.drawString(title, (WIDTH - titleWidth) / 2, 30);

        g.setFont(new Font("Arial", Font.PLAIN, 14));
        int xLabelWidth = g.getFontMetrics().stringWidth(xLabel);
        g.drawString(xLabel, (WIDTH - xLabelWidth) / 2, HEIGHT - 20);

        g.rotate(-Math.PI / 2);
        g.drawString(yLabel, -HEIGHT / 2 - g.getFontMetrics().stringWidth(yLabel) / 2, 20);
        g.rotate(Math.PI / 2);
        for (int i = 0; i <= 5; i++) {
            double value = maxX * i / 5.0;
            String label = String.format("%.2f", value);
            int labelWidth = g.getFontMetrics().stringWidth(label);
            int x_pos = MARGIN + (WIDTH - 2 * MARGIN) * i / 5;
            g.drawString(label, x_pos - labelWidth / 2, HEIGHT - MARGIN + 20);
            g.drawLine(x_pos, HEIGHT - MARGIN, x_pos, HEIGHT - MARGIN + 5);
        }
        for (int i = 0; i <= 5; i++) {
            double value = maxY * i / 5.0;
            String label = String.format("%.1f", value);
            int y_pos = HEIGHT - MARGIN - (HEIGHT - 2 * MARGIN) * i / 5;
            g.drawString(label, MARGIN - 10 - g.getFontMetrics().stringWidth(label), y_pos + 5);
            g.drawLine(MARGIN - 5, y_pos, MARGIN, y_pos);
        }
        g.setColor(Color.GREEN);
        g.setStroke(new BasicStroke(2));
        for (int i = 0; i < x.size() - 1; i++) {
            int x1_pos = MARGIN + (int) ((x.get(i) / maxX) * (WIDTH - 2 * MARGIN));
            int y1_pos = HEIGHT - MARGIN - (int) ((y.get(i) / maxY) * (HEIGHT - 2 * MARGIN));
            int x2_pos = MARGIN + (int) ((x.get(i + 1) / maxX) * (WIDTH - 2 * MARGIN));
            int y2_pos = HEIGHT - MARGIN - (int) ((y.get(i + 1) / maxY) * (HEIGHT - 2 * MARGIN));
            g.drawLine(x1_pos, y1_pos, x2_pos, y2_pos);
        }
    }
}
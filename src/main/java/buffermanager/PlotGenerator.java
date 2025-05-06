package buffermanager;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageIO;

/**
 * Utility class for generating performance plots for Lab 3.
 */
public class PlotGenerator {
    private static final int WIDTH = 1000;
    private static final int HEIGHT = 600;
    private static final int MARGIN = 80;
    private static final String DATA_FILE = "test_results_lab3/performance_chart_data.csv";
    private static final String PERFORMANCE_DATA_FILE = "test_results_lab3/query_performance.csv";

    public static void main(String[] args) throws IOException {
        // Create directory if it doesn't exist
        File resultsDir = new File("test_results_lab3");
        if (!resultsDir.exists()) {
            resultsDir.mkdir();
        }

        // Generate IO comparison plot
        generateIOComparisonPlot();

        // Generate IO Ratio plot
        generateIORatioPlot();
    }

    /**
     * Generates a plot comparing measured and estimated I/O operations.
     */
    private static void generateIOComparisonPlot() throws IOException {
        // Read data from CSV
        List<Double> selectivity = new ArrayList<>();
        List<Long> measuredIO = new ArrayList<>();
        List<Long> estimatedIO = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(DATA_FILE))) {
            // Skip header
            String header = reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                selectivity.add(Double.parseDouble(parts[0]));
                measuredIO.add(Long.parseLong(parts[1]));
                estimatedIO.add(Long.parseLong(parts[2]));
            }
        }

        // Create image
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = image.createGraphics();

        // Fill background
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, WIDTH, HEIGHT);

        // Draw title
        g.setColor(Color.BLACK);
        g.setFont(new Font("Arial", Font.BOLD, 20));
        String title = "I/O Operations vs. Selectivity";
        int titleWidth = g.getFontMetrics().stringWidth(title);
        g.drawString(title, (WIDTH - titleWidth) / 2, 30);

        // Draw axes
        g.setStroke(new BasicStroke(2));
        g.drawLine(MARGIN, HEIGHT - MARGIN, WIDTH - MARGIN, HEIGHT - MARGIN); // X-axis
        g.drawLine(MARGIN, HEIGHT - MARGIN, MARGIN, MARGIN); // Y-axis

        // Label axes
        g.setFont(new Font("Arial", Font.PLAIN, 14));
        g.drawString("Selectivity", WIDTH / 2 - 30, HEIGHT - 20);

        // Rotate for Y-axis label
        g.translate(20, HEIGHT / 2);
        g.rotate(-Math.PI / 2);
        g.drawString("I/O Operations", 0, 0);
        g.rotate(Math.PI / 2);
        g.translate(-20, -HEIGHT / 2);

        // Find max values for scaling
        double maxSelectivity = 0;
        long maxIO = 0;
        for (int i = 0; i < selectivity.size(); i++) {
            maxSelectivity = Math.max(maxSelectivity, selectivity.get(i));
            maxIO = Math.max(maxIO, Math.max(measuredIO.get(i), estimatedIO.get(i)));
        }

        // Round up maxSelectivity to next 0.1
        maxSelectivity = Math.ceil(maxSelectivity * 10) / 10.0;
        // Add some padding to maxIO
        maxIO = (long) (maxIO * 1.1);

        // Draw X-axis ticks and labels
        g.setFont(new Font("Arial", Font.PLAIN, 12));
        for (int i = 0; i <= 10; i++) {
            double tickValue = maxSelectivity * i / 10.0;
            int x = MARGIN + (WIDTH - 2 * MARGIN) * i / 10;
            int y = HEIGHT - MARGIN;
            g.drawLine(x, y, x, y + 5);
            String label = String.format("%.2f", tickValue);
            int labelWidth = g.getFontMetrics().stringWidth(label);
            g.drawString(label, x - labelWidth / 2, y + 20);
        }

        // Draw Y-axis ticks and labels
        for (int i = 0; i <= 10; i++) {
            long tickValue = maxIO * i / 10;
            int x = MARGIN;
            int y = HEIGHT - MARGIN - (HEIGHT - 2 * MARGIN) * i / 10;
            g.drawLine(x - 5, y, x, y);
            String label;
            if (tickValue >= 1000000) {
                label = String.format("%.1fM", tickValue / 1000000.0);
            } else if (tickValue >= 1000) {
                label = String.format("%.1fK", tickValue / 1000.0);
            } else {
                label = String.valueOf(tickValue);
            }
            g.drawString(label, x - 10 - g.getFontMetrics().stringWidth(label), y + 5);
        }

        // Draw measured IO data points and lines
        g.setColor(Color.BLUE);
        g.setStroke(new BasicStroke(3));

        for (int i = 0; i < selectivity.size() - 1; i++) {
            int x1 = MARGIN + (int) ((selectivity.get(i) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y1 = HEIGHT - MARGIN - (int) ((measuredIO.get(i) / (double) maxIO) * (HEIGHT - 2 * MARGIN));
            int x2 = MARGIN + (int) ((selectivity.get(i + 1) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y2 = HEIGHT - MARGIN - (int) ((measuredIO.get(i + 1) / (double) maxIO) * (HEIGHT - 2 * MARGIN));

            g.drawLine(x1, y1, x2, y2);
            g.fillOval(x1 - 4, y1 - 4, 8, 8);
        }

        if (selectivity.size() > 0) {
            int lastIndex = selectivity.size() - 1;
            int x = MARGIN + (int) ((selectivity.get(lastIndex) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y = HEIGHT - MARGIN - (int) ((measuredIO.get(lastIndex) / (double) maxIO) * (HEIGHT - 2 * MARGIN));
            g.fillOval(x - 4, y - 4, 8, 8);
        }

        // Draw estimated IO data points and lines
        g.setColor(Color.RED);
        g.setStroke(new BasicStroke(3, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, new float[] { 9 }, 0));

        for (int i = 0; i < selectivity.size() - 1; i++) {
            int x1 = MARGIN + (int) ((selectivity.get(i) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y1 = HEIGHT - MARGIN - (int) ((estimatedIO.get(i) / (double) maxIO) * (HEIGHT - 2 * MARGIN));
            int x2 = MARGIN + (int) ((selectivity.get(i + 1) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y2 = HEIGHT - MARGIN - (int) ((estimatedIO.get(i + 1) / (double) maxIO) * (HEIGHT - 2 * MARGIN));

            g.drawLine(x1, y1, x2, y2);
            g.fillOval(x1 - 4, y1 - 4, 8, 8);
        }

        if (selectivity.size() > 0) {
            int lastIndex = selectivity.size() - 1;
            int x = MARGIN + (int) ((selectivity.get(lastIndex) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y = HEIGHT - MARGIN - (int) ((estimatedIO.get(lastIndex) / (double) maxIO) * (HEIGHT - 2 * MARGIN));
            g.fillOval(x - 4, y - 4, 8, 8);
        }

        // Add legend
        g.setColor(Color.WHITE);
        g.fillRect(WIDTH - 220, 60, 180, 60);
        g.setColor(Color.BLACK);
        g.drawRect(WIDTH - 220, 60, 180, 60);

        g.setColor(Color.BLUE);
        g.setStroke(new BasicStroke(3));
        g.drawLine(WIDTH - 200, 80, WIDTH - 150, 80);
        g.setColor(Color.BLACK);
        g.drawString("Measured I/O", WIDTH - 140, 85);

        g.setColor(Color.RED);
        g.setStroke(new BasicStroke(3, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, new float[] { 9 }, 0));
        g.drawLine(WIDTH - 200, 110, WIDTH - 150, 110);
        g.setColor(Color.BLACK);
        g.drawString("Estimated I/O", WIDTH - 140, 115);

        // Save the plot
        File outputFile = new File("test_results_lab3/io_comparison_plot.png");
        ImageIO.write(image, "png", outputFile);
        System.out.println("Generated I/O comparison plot: " + outputFile.getAbsolutePath());
    }

    /**
     * Generates a plot showing the ratio of measured to estimated I/O.
     */
    private static void generateIORatioPlot() throws IOException {
        // Read data from CSV
        List<Double> selectivity = new ArrayList<>();
        List<Double> ioRatios = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(DATA_FILE))) {
            // Skip header
            String header = reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                double sel = Double.parseDouble(parts[0]);
                long measured = Long.parseLong(parts[1]);
                long estimated = Long.parseLong(parts[2]);

                selectivity.add(sel);
                // Calculate ratio (avoid division by zero)
                double ratio = (estimated > 0) ? (double) measured / estimated : 0;
                ioRatios.add(ratio);
            }
        }

        // Create image
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = image.createGraphics();

        // Fill background
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, WIDTH, HEIGHT);

        // Draw title
        g.setColor(Color.BLACK);
        g.setFont(new Font("Arial", Font.BOLD, 20));
        String title = "I/O Ratio (Measured/Estimated) vs. Selectivity";
        int titleWidth = g.getFontMetrics().stringWidth(title);
        g.drawString(title, (WIDTH - titleWidth) / 2, 30);

        // Draw axes
        g.setStroke(new BasicStroke(2));
        g.drawLine(MARGIN, HEIGHT - MARGIN, WIDTH - MARGIN, HEIGHT - MARGIN); // X-axis
        g.drawLine(MARGIN, HEIGHT - MARGIN, MARGIN, MARGIN); // Y-axis

        // Label axes
        g.setFont(new Font("Arial", Font.PLAIN, 14));
        g.drawString("Selectivity", WIDTH / 2 - 30, HEIGHT - 20);

        // Rotate for Y-axis label
        g.translate(20, HEIGHT / 2);
        g.rotate(-Math.PI / 2);
        g.drawString("I/O Ratio (Measured/Estimated)", 0, 0);
        g.rotate(Math.PI / 2);
        g.translate(-20, -HEIGHT / 2);

        // Find max values for scaling
        double maxSelectivity = 0;
        double maxRatio = 0;
        for (int i = 0; i < selectivity.size(); i++) {
            maxSelectivity = Math.max(maxSelectivity, selectivity.get(i));
            maxRatio = Math.max(maxRatio, ioRatios.get(i));
        }

        // Round up maxSelectivity to next 0.1
        maxSelectivity = Math.ceil(maxSelectivity * 10) / 10.0;
        // Add some padding to maxRatio and ensure we have a reasonable minimum
        maxRatio = Math.max(2.0, Math.ceil(maxRatio * 10) / 10.0 * 1.1);

        // Draw X-axis ticks and labels
        g.setFont(new Font("Arial", Font.PLAIN, 12));
        for (int i = 0; i <= 10; i++) {
            double tickValue = maxSelectivity * i / 10.0;
            int x = MARGIN + (WIDTH - 2 * MARGIN) * i / 10;
            int y = HEIGHT - MARGIN;
            g.drawLine(x, y, x, y + 5);
            String label = String.format("%.2f", tickValue);
            int labelWidth = g.getFontMetrics().stringWidth(label);
            g.drawString(label, x - labelWidth / 2, y + 20);
        }

        // Draw Y-axis ticks and labels
        for (int i = 0; i <= 10; i++) {
            double tickValue = maxRatio * i / 10.0;
            int x = MARGIN;
            int y = HEIGHT - MARGIN - (HEIGHT - 2 * MARGIN) * i / 10;
            g.drawLine(x - 5, y, x, y);
            String label = String.format("%.2f", tickValue);
            g.drawString(label, x - 10 - g.getFontMetrics().stringWidth(label), y + 5);
        }

        // Draw reference line at ratio = 1.0
        g.setColor(Color.GRAY);
        g.setStroke(new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, new float[] { 5 }, 0));
        int yRef = HEIGHT - MARGIN - (int) ((1.0 / maxRatio) * (HEIGHT - 2 * MARGIN));
        g.drawLine(MARGIN, yRef, WIDTH - MARGIN, yRef);

        // Draw ratio data points and lines
        g.setColor(Color.GREEN);
        g.setStroke(new BasicStroke(3));

        for (int i = 0; i < selectivity.size() - 1; i++) {
            int x1 = MARGIN + (int) ((selectivity.get(i) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y1 = HEIGHT - MARGIN - (int) ((ioRatios.get(i) / maxRatio) * (HEIGHT - 2 * MARGIN));
            int x2 = MARGIN + (int) ((selectivity.get(i + 1) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y2 = HEIGHT - MARGIN - (int) ((ioRatios.get(i + 1) / maxRatio) * (HEIGHT - 2 * MARGIN));

            g.drawLine(x1, y1, x2, y2);
            g.fillOval(x1 - 4, y1 - 4, 8, 8);
        }

        if (selectivity.size() > 0) {
            int lastIndex = selectivity.size() - 1;
            int x = MARGIN + (int) ((selectivity.get(lastIndex) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y = HEIGHT - MARGIN - (int) ((ioRatios.get(lastIndex) / maxRatio) * (HEIGHT - 2 * MARGIN));
            g.fillOval(x - 4, y - 4, 8, 8);
        }

        // Add legend and explanation
        g.setColor(Color.WHITE);
        g.fillRect(WIDTH - 300, 60, 260, 100);
        g.setColor(Color.BLACK);
        g.drawRect(WIDTH - 300, 60, 260, 100);

        g.setColor(Color.GREEN);
        g.setStroke(new BasicStroke(3));
        g.drawLine(WIDTH - 280, 80, WIDTH - 230, 80);
        g.fillOval(WIDTH - 255, 76, 8, 8);

        g.setColor(Color.BLACK);
        g.drawString("I/O Ratio (Measured/Estimated)", WIDTH - 220, 85);

        g.setColor(Color.GRAY);
        g.setStroke(new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, new float[] { 5 }, 0));
        g.drawLine(WIDTH - 280, 110, WIDTH - 230, 110);

        g.setColor(Color.BLACK);
        g.drawString("Perfect Estimation (Ratio = 1.0)", WIDTH - 220, 115);

        g.drawString("Ratio > 1: Underestimation", WIDTH - 280, 140);
        g.drawString("Ratio < 1: Overestimation", WIDTH - 280, 155);

        // Save the plot
        File outputFile = new File("test_results_lab3/io_ratio_plot.png");
        ImageIO.write(image, "png", outputFile);
        System.out.println("Generated I/O ratio plot: " + outputFile.getAbsolutePath());

        // Calculate and display statistics
        double sumRatio = 0;
        double minRatio = Double.MAX_VALUE;
        double maxRatioValue = 0;
        for (double ratio : ioRatios) {
            sumRatio += ratio;
            minRatio = Math.min(minRatio, ratio);
            maxRatioValue = Math.max(maxRatioValue, ratio);
        }
        double avgRatio = ioRatios.size() > 0 ? sumRatio / ioRatios.size() : 0;

        System.out.println("I/O Ratio Statistics:");
        System.out.printf("Average Ratio: %.2f%n", avgRatio);
        System.out.printf("Min Ratio: %.2f%n", minRatio);
        System.out.printf("Max Ratio: %.2f%n", maxRatioValue);

        if (avgRatio < 0.9) {
            System.out.println("The formula tends to overestimate I/O operations.");
        } else if (avgRatio > 1.1) {
            System.out.println("The formula tends to underestimate I/O operations.");
        } else {
            System.out.println("The formula provides a reasonable estimation of I/O operations.");
        }
    }
}
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

/**
 * Utility for generating performance charts from the query execution results.
 */
public class PlotGenerator {
    private static final int WIDTH = 1000;
    private static final int HEIGHT = 600;
    private static final int MARGIN = 80;
    private static final String DATA_FILE = "performance_chart_data.csv";

    public static void main(String[] args) {
        try {
            generateIoComparisonPlot();
            generateRatioPlot();
            System.out.println("Performance plots generated successfully.");
        } catch (IOException e) {
            System.err.println("Error generating plots: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Generates a plot comparing measured I/O versus estimated I/O for different
     * selectivity levels.
     */
    private static void generateIoComparisonPlot() throws IOException {
        List<Double> selectivity = new ArrayList<>();
        List<Long> measuredIo = new ArrayList<>();
        List<Long> estimatedIo = new ArrayList<>();

        // Read data from the performance data file
        try (BufferedReader reader = new BufferedReader(new FileReader(DATA_FILE))) {
            String line = reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                selectivity.add(Double.parseDouble(parts[0]));
                measuredIo.add(Long.parseLong(parts[1]));
                estimatedIo.add(Long.parseLong(parts[2]));
            }
        }

        // Create the plot
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = image.createGraphics();

        // Set white background
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, WIDTH, HEIGHT);

        // Plot title
        g.setColor(Color.BLACK);
        g.setFont(new Font("Arial", Font.BOLD, 20));
        String title = "I/O Operations vs. Selectivity";
        int titleWidth = g.getFontMetrics().stringWidth(title);
        g.drawString(title, (WIDTH - titleWidth) / 2, 30);

        // Draw axes
        g.setStroke(new BasicStroke(2));
        g.drawLine(MARGIN, HEIGHT - MARGIN, WIDTH - MARGIN, HEIGHT - MARGIN); // X-axis
        g.drawLine(MARGIN, HEIGHT - MARGIN, MARGIN, MARGIN); // Y-axis

        // X-axis label
        g.setFont(new Font("Arial", Font.PLAIN, 16));
        String xLabel = "Selectivity";
        int xLabelWidth = g.getFontMetrics().stringWidth(xLabel);
        g.drawString(xLabel, (WIDTH - xLabelWidth) / 2, HEIGHT - 20);

        // Y-axis label
        g.rotate(-Math.PI / 2);
        String yLabel = "I/O Operations";
        int yLabelWidth = g.getFontMetrics().stringWidth(yLabel);
        g.drawString(yLabel, -(HEIGHT + yLabelWidth) / 2, 25);
        g.rotate(Math.PI / 2);

        // Calculate scale
        double maxSelectivity = 0;
        long maxIo = 0;

        for (int i = 0; i < selectivity.size(); i++) {
            maxSelectivity = Math.max(maxSelectivity, selectivity.get(i));
            maxIo = Math.max(maxIo, Math.max(measuredIo.get(i), estimatedIo.get(i)));
        }

        // Add some padding to max values
        maxSelectivity = Math.ceil(maxSelectivity * 10) / 10.0;
        maxIo = (long) (maxIo * 1.1);

        // Draw X-axis ticks and labels
        g.setFont(new Font("Arial", Font.PLAIN, 12));
        for (int i = 0; i <= 10; i++) {
            double value = maxSelectivity * i / 10.0;
            String label = String.format("%.2f", value);
            int labelWidth = g.getFontMetrics().stringWidth(label);
            int x = MARGIN + (WIDTH - 2 * MARGIN) * i / 10;
            g.drawLine(x, HEIGHT - MARGIN, x, HEIGHT - MARGIN + 5);
            g.drawString(label, x - labelWidth / 2, HEIGHT - MARGIN + 20);
        }

        // Draw Y-axis ticks and labels
        for (int i = 0; i <= 10; i++) {
            long value = maxIo * i / 10;
            String label;
            if (value >= 1000000) {
                label = String.format("%.1fM", value / 1000000.0);
            } else if (value >= 1000) {
                label = String.format("%.1fK", value / 1000.0);
            } else {
                label = String.valueOf(value);
            }
            int labelWidth = g.getFontMetrics().stringWidth(label);
            int y = HEIGHT - MARGIN - (HEIGHT - 2 * MARGIN) * i / 10;
            g.drawLine(MARGIN - 5, y, MARGIN, y);
            g.drawString(label, MARGIN - 10 - labelWidth, y + 5);
        }

        // Plot measured I/O
        g.setColor(Color.BLUE);
        g.setStroke(new BasicStroke(3));

        for (int i = 0; i < selectivity.size() - 1; i++) {
            int x1 = MARGIN + (int) ((selectivity.get(i) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y1 = HEIGHT - MARGIN - (int) ((measuredIo.get(i) / (double) maxIo) * (HEIGHT - 2 * MARGIN));
            int x2 = MARGIN + (int) ((selectivity.get(i + 1) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y2 = HEIGHT - MARGIN - (int) ((measuredIo.get(i + 1) / (double) maxIo) * (HEIGHT - 2 * MARGIN));
            g.drawLine(x1, y1, x2, y2);
        }

        // Plot estimated I/O
        g.setColor(Color.RED);
        g.setStroke(new BasicStroke(3, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, new float[] { 9 }, 0));

        for (int i = 0; i < selectivity.size() - 1; i++) {
            int x1 = MARGIN + (int) ((selectivity.get(i) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y1 = HEIGHT - MARGIN - (int) ((estimatedIo.get(i) / (double) maxIo) * (HEIGHT - 2 * MARGIN));
            int x2 = MARGIN + (int) ((selectivity.get(i + 1) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y2 = HEIGHT - MARGIN - (int) ((estimatedIo.get(i + 1) / (double) maxIo) * (HEIGHT - 2 * MARGIN));
            g.drawLine(x1, y1, x2, y2);
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
        File outputFile = new File("io_comparison_plot.png");
        ImageIO.write(image, "png", outputFile);
        System.out.println("Generated I/O comparison plot: " + outputFile.getAbsolutePath());
    }

    /**
     * Generates a plot showing the ratio of measured I/O to estimated I/O.
     */
    private static void generateRatioPlot() throws IOException {
        List<Double> selectivity = new ArrayList<>();
        List<Double> ratios = new ArrayList<>();

        // Read data from the performance data file
        try (BufferedReader reader = new BufferedReader(new FileReader(DATA_FILE))) {
            String line = reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                double sel = Double.parseDouble(parts[0]);
                long measured = Long.parseLong(parts[1]);
                long estimated = Long.parseLong(parts[2]);

                selectivity.add(sel);
                double ratio = estimated > 0 ? (double) measured / estimated : 0;
                ratios.add(ratio);
            }
        }

        // Create the plot
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = image.createGraphics();

        // Set white background
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, WIDTH, HEIGHT);

        // Plot title
        g.setColor(Color.BLACK);
        g.setFont(new Font("Arial", Font.BOLD, 20));
        String title = "Ratio of Measured I/O to Estimated I/O vs. Selectivity";
        int titleWidth = g.getFontMetrics().stringWidth(title);
        g.drawString(title, (WIDTH - titleWidth) / 2, 30);

        // Draw axes
        g.setStroke(new BasicStroke(2));
        g.drawLine(MARGIN, HEIGHT - MARGIN, WIDTH - MARGIN, HEIGHT - MARGIN); // X-axis
        g.drawLine(MARGIN, HEIGHT - MARGIN, MARGIN, MARGIN); // Y-axis

        // X-axis label
        g.setFont(new Font("Arial", Font.PLAIN, 16));
        String xLabel = "Selectivity";
        int xLabelWidth = g.getFontMetrics().stringWidth(xLabel);
        g.drawString(xLabel, (WIDTH - xLabelWidth) / 2, HEIGHT - 20);

        // Y-axis label
        g.rotate(-Math.PI / 2);
        String yLabel = "Ratio (Measured / Estimated)";
        int yLabelWidth = g.getFontMetrics().stringWidth(yLabel);
        g.drawString(yLabel, -(HEIGHT + yLabelWidth) / 2, 25);
        g.rotate(Math.PI / 2);

        // Calculate scale
        double maxSelectivity = 0;
        double maxRatio = 0;

        for (int i = 0; i < selectivity.size(); i++) {
            maxSelectivity = Math.max(maxSelectivity, selectivity.get(i));
            maxRatio = Math.max(maxRatio, ratios.get(i));
        }

        // Add some padding to max values
        maxSelectivity = Math.ceil(maxSelectivity * 10) / 10.0;
        maxRatio = Math.ceil(maxRatio * 10) / 10.0;
        if (maxRatio < 2.0)
            maxRatio = 2.0; // Ensure we show at least up to ratio 2.0

        // Draw X-axis ticks and labels
        g.setFont(new Font("Arial", Font.PLAIN, 12));
        for (int i = 0; i <= 10; i++) {
            double value = maxSelectivity * i / 10.0;
            String label = String.format("%.2f", value);
            int labelWidth = g.getFontMetrics().stringWidth(label);
            int x = MARGIN + (WIDTH - 2 * MARGIN) * i / 10;
            g.drawLine(x, HEIGHT - MARGIN, x, HEIGHT - MARGIN + 5);
            g.drawString(label, x - labelWidth / 2, HEIGHT - MARGIN + 20);
        }

        // Draw Y-axis ticks and labels
        for (int i = 0; i <= 10; i++) {
            double value = maxRatio * i / 10.0;
            String label = String.format("%.1f", value);
            int labelWidth = g.getFontMetrics().stringWidth(label);
            int y = HEIGHT - MARGIN - (HEIGHT - 2 * MARGIN) * i / 10;
            g.drawLine(MARGIN - 5, y, MARGIN, y);
            g.drawString(label, MARGIN - 10 - labelWidth, y + 5);
        }

        // Add a reference line at ratio = 1.0
        g.setColor(Color.GRAY);
        g.setStroke(new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, new float[] { 5 }, 0));
        int y_ratio_1 = HEIGHT - MARGIN - (int) ((1.0 / maxRatio) * (HEIGHT - 2 * MARGIN));
        g.drawLine(MARGIN, y_ratio_1, WIDTH - MARGIN, y_ratio_1);

        // Plot ratio
        g.setColor(Color.GREEN.darker());
        g.setStroke(new BasicStroke(3));

        for (int i = 0; i < selectivity.size() - 1; i++) {
            int x1 = MARGIN + (int) ((selectivity.get(i) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y1 = HEIGHT - MARGIN - (int) ((ratios.get(i) / maxRatio) * (HEIGHT - 2 * MARGIN));
            int x2 = MARGIN + (int) ((selectivity.get(i + 1) / maxSelectivity) * (WIDTH - 2 * MARGIN));
            int y2 = HEIGHT - MARGIN - (int) ((ratios.get(i + 1) / maxRatio) * (HEIGHT - 2 * MARGIN));
            g.drawLine(x1, y1, x2, y2);
        }

        // Add explanatory text
        g.setColor(Color.BLACK);
        g.setFont(new Font("Arial", Font.PLAIN, 12));
        g.drawString("Ratio = 1.0: Formula perfectly predicts actual I/O", MARGIN + 10, y_ratio_1 - 10);
        g.drawString("Ratio > 1.0: Actual I/O exceeds predicted I/O", MARGIN + 10, y_ratio_1 - 30);
        g.drawString("Ratio < 1.0: Actual I/O is less than predicted I/O", MARGIN + 10, y_ratio_1 + 20);

        // Save the plot
        File outputFile = new File("io_ratio_plot.png");
        ImageIO.write(image, "png", outputFile);
        System.out.println("Generated I/O ratio plot: " + outputFile.getAbsolutePath());
    }
}
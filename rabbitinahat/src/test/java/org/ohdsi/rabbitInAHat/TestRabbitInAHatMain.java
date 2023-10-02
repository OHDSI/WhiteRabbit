package org.ohdsi.rabbitInAHat;

import com.github.caciocavallosilano.cacio.ctc.junit.CacioAssertJRunner;
import org.assertj.swing.annotation.GUITest;
import org.assertj.swing.core.ComponentDragAndDrop;
import org.assertj.swing.edt.GuiActionRunner;
import org.assertj.swing.finder.JFileChooserFinder;
import org.assertj.swing.fixture.FrameFixture;
import org.assertj.swing.fixture.JFileChooserFixture;
import org.assertj.swing.image.ScreenshotTaker;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.awt.*;
import java.awt.event.KeyEvent;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;
import static org.ohdsi.rabbitInAHat.RabbitInAHatMain.*;

/*
 * CacioTestRunner enables running the Swing GUI tests in a virtual screen. This allows the integration tests to run
 * anywhere without being blocked by the absence of a real screen (e.g. github actions), and without being
 * disrupted by unrelated user activity on workstations/laptops (any keyboard or mouse action).
 * For debugging purposes, you can disable the annotation below to have the tests run on your screen. Be aware that
 * any interaction with mouse or keyboard can (will) disrupt the tests if they run on your screen.
 */
@RunWith(CacioAssertJRunner.class)
public class TestRabbitInAHatMain {

    private static FrameFixture window;

    private final static int WIDTH = 1920;
    private final static int HEIGHT = 1080;
    @BeforeClass
    public static void setupOnce() {
        System.setProperty("cacio.managed.screensize", String.format("%sx%s", WIDTH, HEIGHT));
    }

    @Before
    public void setUp() {
        String[] args = {};
        RabbitInAHatMain rabbitInAHatMain = GuiActionRunner.execute(() -> new RabbitInAHatMain(args));
        window = new FrameFixture(rabbitInAHatMain.getFrame());
        window.splitPane("splitpane").target().setDividerLocation(WIDTH / 2);
        window.show(); // shows the frame to test
    }

    @After
    public void tearDown() {
        // uncertain if the assertions with isActive() are sufficient, but at least it's an indication the open window
        // did close
        if (window.target().isActive()) {
            window.menuItem(ACTION_EXIT).click();                   // select Exit from menu
            window.optionPane().buttonWithText("Yes").click();      // confirm that we want to exit
            assertFalse(window.target().isActive());                // main window is now inactive
        }
        window.robot().cleanUp();                                   // otherwise subsequent tests will get stuck
    }

    @Test
    public void openApp() {
        // this just opens and closes the application. No point in trying anything else if this does not work
        assertTrue(true);
    }

    @Test
    public void openReport() throws URISyntaxException {
        window.menuItem(ACTION_OPEN_SCAN_REPORT).click();
        JFileChooserFixture fileChooser = JFileChooserFinder.findFileChooser().using(window.robot());
        assertEquals(TITLE_SELECT_FILE, fileChooser.target().getDialogTitle());
        URL scanReportUrl = this.getClass().getClassLoader().getResource("examples/test_scanreports/ScanReport_minimal.xlsx");
        fileChooser.selectFile(new File(Objects.requireNonNull(scanReportUrl).toURI())).approve();
    }

    @GUITest
    @Test
    public void openAndVerifySavedETLSpecs() throws URISyntaxException {
        // open the test ETL specification
        openETLSpecs("scan/etl-specs.json.gz");
        MappingPanel tablesPanel = getTablesPanel();

        assertEquals("7 mappings are expected", 7, tablesPanel.getArrows().size());

        // verify the mappings
        verifyTableMapping(tablesPanel, "patients.csv", "person");
        verifyTableMapping(tablesPanel, "claims.csv", "person");
        verifyTableMapping(tablesPanel, "conditions.csv", "observation");
        verifyTableMapping(tablesPanel, "conditions.csv", "condition_occurrence");
        verifyTableMapping(tablesPanel, "medications.csv", "drug_exposure");
        verifyTableMapping(tablesPanel, "encounters.csv", "observation_period");
        verifyTableMapping(tablesPanel, "encounters.csv", "visit_occurrence");
    }

    @GUITest
    @Test
    public void createTableMapping() throws URISyntaxException {
        openETLSpecs("scan/etl-specs.json.gz");
        MappingPanel tablesPanel = getTablesPanel();
        createAndVerifyTableMapping(tablesPanel, "devices.csv", "device_exposure");
    }

    private void openETLSpecs(String specName) throws URISyntaxException {
        window.menuItem(ACTION_OPEN_ETL_SPECS).click();
        JFileChooserFixture fileChooser = JFileChooserFinder.findFileChooser().using(window.robot());
        assertEquals(TITLE_SELECT_FILE, fileChooser.target().getDialogTitle());
        URL etlSpecsUrl = this.getClass().getClassLoader().getResource(specName);
        fileChooser.selectFile(new File(Objects.requireNonNull(etlSpecsUrl).toURI())).approve();
        MappingPanel tablesPanel = window.panel(PANEL_TABLE_MAPPING).targetCastedTo(MappingPanel.class);
        assertTrue("There should be source items", tablesPanel.getVisibleSourceComponents().size() > 0);
        assertTrue("There should be target items", tablesPanel.getVisibleTargetComponents().size() > 0);
    }
    private void verifyTableMapping(MappingPanel tablesPanel, String sourceName, String targetName) {
        LabeledRectangle sourceTable = findMappableItem(tablesPanel.getVisibleSourceComponents(), sourceName);
        LabeledRectangle targetTable = findMappableItem(tablesPanel.getVisibleTargetComponents(), targetName);
        assertFalse(sourceTable.isSelected());
        assertFalse(targetTable.isSelected());

        Arrow mapping = findMapping(tablesPanel.getArrows(), sourceName, targetName);

        assertEquals(Arrow.HighlightStatus.NONE_SELECTED, mapping.getHighlightStatus());
        clickAndVerifyLabeledRectangles(tablesPanel, sourceTable);
        assertEquals(Arrow.HighlightStatus.SOURCE_SELECTED, mapping.getHighlightStatus());

        clickAndVerifyLabeledRectangles(tablesPanel, targetTable);
        assertEquals(Arrow.HighlightStatus.TARGET_SELECTED, mapping.getHighlightStatus());

        deselectAll(tablesPanel);
        clickAndVerifyLabeledRectangles(tablesPanel, sourceTable, targetTable);
        assertEquals(Arrow.HighlightStatus.BOTH_SELECTED, mapping.getHighlightStatus());

        deselectAll(tablesPanel);
    }

    private void clickAndVerifyLabeledRectangles(MappingPanel tablesPanel, LabeledRectangle... rectangles) {
        Arrays.stream(rectangles).forEach(r -> {
            assertFalse(r.isSelected());
            if (rectangles.length > 1) {
                window.robot().pressKey(KeyEvent.VK_SHIFT);
            }
            window.robot().click(tablesPanel, new Point(r.getX() + 1, r.getY() + 1));
            if (rectangles.length > 1) {
                window.robot().releaseKey(KeyEvent.VK_SHIFT);
            }
//            if (!r.isSelected()) {
//                ScreenshotTaker screenshotTaker = new ScreenshotTaker();
//                screenshotTaker.saveDesktopAsPng("problem.png");
//                System.out.println("Problem!");
//            }
            assertTrue(r.isSelected());
        });
    }

    LabeledRectangle findMappableItem(List<LabeledRectangle> items, String name) {
        LabeledRectangle[] matchingItems = items.stream().filter(i -> i.getItem().getName().equals(name)).toArray(LabeledRectangle[]::new);
        assertEquals(String.format("There should be exactly 1 item with name %s", name), 1, matchingItems.length);

        return matchingItems[0];
    }

    Arrow findMapping(List<Arrow> mappings, String source, String target) {
        Arrow[] matchingMappings = mappings.stream().filter(m -> m.getSource().getItem().getName().equals(source) &&
                m.getTarget().getItem().getName().equals(target)).toArray(Arrow[]::new);
        assertEquals(String.format("Mapping from %s to %s is expected to exists", source, target),
                1, matchingMappings.length);

        return matchingMappings[0];
    }

    private void deselectAll(MappingPanel tablesPanel) {
        tablesPanel.getVisibleSourceComponents().forEach(l -> l.setSelected(false));
        tablesPanel.getVisibleTargetComponents().forEach(l -> l.setSelected(false));
        tablesPanel.getArrows().forEach(a -> a.setSelected(false));
    }

    private void createAndVerifyTableMapping(MappingPanel tablesPanel, String source, String target) {
        // pre: source and target must not be connected
        assertEquals(String.format("A mapping between source '%s' and target '%s' should not yet exist.", source, target),
                0,
                 tablesPanel.getArrows()
                         .stream()
                         .filter(a -> a.getSource().getItem().getName().equalsIgnoreCase(source) &&
                                      a.getTarget().getItem().getName().equalsIgnoreCase(target)).count());

        // action: drag the arrowhead at sourceItem to targetItem
        LabeledRectangle sourceItem = findMappableItem(tablesPanel.getVisibleSourceComponents(), source);
        LabeledRectangle targetItem = findMappableItem(tablesPanel.getVisibleTargetComponents(), target);

        ComponentDragAndDrop dragAndDrop = new ComponentDragAndDrop(window.robot());
        dragAndDrop.drag(tablesPanel, arrowHeadLocation(sourceItem));
        dragAndDrop.drop(tablesPanel, new Point(targetItem.getX(), targetItem.getY()));

        // post: there should be a mapping between source and target
        verifyTableMapping(tablesPanel, source, target);
    }

    private Point arrowHeadLocation(LabeledRectangle item) {
        return new Point(item.getX() + item.getWidth() + (Arrow.headThickness / 2), item.getY() + item.getHeight() / 2);
    }

    private MappingPanel getTablesPanel() {
        return window.panel(PANEL_TABLE_MAPPING).targetCastedTo(MappingPanel.class);
    }
}
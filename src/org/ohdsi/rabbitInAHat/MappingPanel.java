/*******************************************************************************
 * Copyright 2016 Observational Health Data Sciences and Informatics
 * 
 * This file is part of WhiteRabbit
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.rabbitInAHat;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Hashtable;

import javax.swing.AbstractAction;
import javax.swing.JPanel;
import javax.swing.KeyStroke;

import org.ohdsi.rabbitInAHat.Arrow.HighlightStatus;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.Mapping;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.collections.IntegerComparator;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class MappingPanel extends JPanel implements MouseListener, MouseMotionListener {

	private static final long		serialVersionUID			= 4589294949568810155L;

	public static int				ITEM_HEIGHT					= 50;
	public static int				ITEM_WIDTH					= 200;
	public static int				MARGIN						= 10;
	public static int				HEADER_HEIGHT				= 25;
	public static int				HEADER_TOP_MARGIN			= 0;
	public static int				MIN_SPACE_BETWEEN_COLUMNS	= 200;
	public static int				ARROW_START_WIDTH			= 50;
	public static int				BORDER_HEIGHT				= 25;

	private int						sourceX						= 10;
	private int						cdmX						= 200;

	private Mapping<?>				mapping;
	private List<LabeledRectangle>	sourceComponents			= new ArrayList<LabeledRectangle>();
	private List<LabeledRectangle>	cdmComponents				= new ArrayList<LabeledRectangle>();
	private List<Arrow>				arrows						= new ArrayList<Arrow>();
	private LabeledRectangle		dragRectangle				= null;
	private LabeledRectangle		lastSelectedRectangle		= null;
	private Arrow					dragArrow					= null;
	private Arrow					zoomArrow					= null;
	private Arrow					selectedArrow				= null;
	private LabeledRectangle		dragArrowPreviousTarget		= null;
	private int						dragOffsetY;
	private int						maxHeight					= Integer.MAX_VALUE;
	private boolean					minimized					= false;
	private MappingPanel			slaveMappingPanel;
	private boolean					showOnlyConnectedItems		= false;

	private int						shortcutMask				= Toolkit.getDefaultToolkit().getMenuShortcutKeyMask();

	private String					lastSourceFilter			= "";
	private String					lastTargetFilter			= "";

	private boolean					showingArrowStarts			= false;

	private DetailsListener			detailsListener;

	@SuppressWarnings("serial")
	public MappingPanel(Mapping<?> mapping) {
		super();
		this.mapping = mapping;
		this.setFocusable(true);
		addMouseListener(this);
		addMouseMotionListener(this);

		// Add keybindings to delete arrows
		this.getInputMap(WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_DELETE, 0, false), "del pressed");
		this.getInputMap(WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_BACK_SPACE, 0, false), "del pressed");
		this.getActionMap().put("del pressed", new AbstractAction() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (selectedArrow != null) {
					removeArrow(selectedArrow);
				}
			}
		});

		renderModel();
	}

	public String getLastSourceFilter() {
		return lastSourceFilter;
	}

	public String getLastTargetFilter() {
		return lastTargetFilter;
	}

	public boolean isMinimized() {
		return minimized;
	}

	public void setMapping(Mapping<?> mapping) {
		maximize();
		this.mapping = mapping;
		renderModel();
	}

	public List<LabeledRectangle> getVisibleSourceComponents() {
		return getVisibleRectangles(sourceComponents);
	}

	public List<LabeledRectangle> getVisibleTargetComponents() {
		return getVisibleRectangles(cdmComponents);
	}

	public void setSlaveMappingPanel(MappingPanel mappingPanel) {
		this.slaveMappingPanel = mappingPanel;
	}

	public MappingPanel getSlaveMappingPanel() {
		return this.slaveMappingPanel;
	}

	public void setShowOnlyConnectedItems(boolean value) {
		showOnlyConnectedItems = value;
		renderModel();
	}

	private void renderModel() {
		sourceComponents.clear();
		cdmComponents.clear();
		arrows.clear();
		for (MappableItem item : mapping.getSourceItems())
			if (!showOnlyConnectedItems || isConnected(item)) {
				if (item.isStem())
					sourceComponents.add(new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(160, 0, 160)));
				else
					sourceComponents.add(new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(255, 128, 0)));
			}
		for (MappableItem item : mapping.getTargetItems())
			if (!showOnlyConnectedItems || isConnected(item)) {
				if (item.isStem())
					cdmComponents.add(new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(160, 0, 160)));
				else
					cdmComponents.add(new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(128, 128, 255)));
			}
		for (ItemToItemMap map : mapping.getSourceToTargetMaps()) {
			Arrow component = new Arrow(getComponentWithItem(map.getSourceItem(), sourceComponents), getComponentWithItem(map.getTargetItem(), cdmComponents),
					map);
			arrows.add(component);
		}
		layoutItems();
		repaint();
	}

	private boolean isConnected(MappableItem item) {
		for (ItemToItemMap map : mapping.getSourceToTargetMaps())
			if (map.getSourceItem() == item || map.getTargetItem() == item)
				return true;
		return false;
	}

	public void markCompleted() {
		for (Arrow arrow : arrows) {
			if (arrow.isSelected() || arrow.getHighlightStatus() == HighlightStatus.BOTH_SELECTED) {
				arrow.getItemToItemMap().setCompleted(true);
			}
		}
		repaint();
	}

	public void unmarkCompleted() {
		for (Arrow arrow : arrows) {
			if (arrow.isSelected() || arrow.getHighlightStatus() == HighlightStatus.BOTH_SELECTED) {
				arrow.getItemToItemMap().setCompleted(false);
			}
		}
		repaint();
	}

	private LabeledRectangle getComponentWithItem(MappableItem item, List<LabeledRectangle> components) {
		for (LabeledRectangle component : components)
			if (component.getItem().equals(item))
				return component;
		return null;
	}

	// Layout visible LabeledRectangles
	private void layoutItems() {
		if (minimized) { // Only update x coordinate
			for (LabeledRectangle targetComponent : getVisibleTargetComponents()) {
				targetComponent.setLocation(cdmX, targetComponent.getY());
			}
		} else {
			setLabeledRectanglesLocation(getVisibleSourceComponents(), sourceX);
			setLabeledRectanglesLocation(getVisibleTargetComponents(), cdmX);
		}
	}

	// Sets the location of the Labeled Rectangles
	private void setLabeledRectanglesLocation(List<LabeledRectangle> components, int xpos) {
		int avoidY = Integer.MAX_VALUE;
		if (dragRectangle != null && dragRectangle.getX() == xpos)
			avoidY = dragRectangle.getY();
		int y = HEADER_HEIGHT + HEADER_TOP_MARGIN;
		for (int i = 0; i < components.size(); i++) {
			LabeledRectangle item = components.get(i);
			if (y > avoidY - ITEM_HEIGHT && y <= avoidY + MARGIN)
				y += MARGIN + ITEM_HEIGHT;

			if (dragRectangle == null || item != dragRectangle) {
				item.setLocation(xpos, y);
				y += MARGIN + ITEM_HEIGHT;
			}

		}
	}

	public Dimension getMinimumSize() {
		Dimension dimension = new Dimension();
		dimension.width = 2 * (ITEM_WIDTH + MARGIN) + MIN_SPACE_BETWEEN_COLUMNS;
		dimension.height = Math.min(HEADER_HEIGHT + HEADER_TOP_MARGIN + Math.max(sourceComponents.size(), cdmComponents.size()) * (ITEM_HEIGHT + MARGIN),
				maxHeight);

		return dimension;
	}

	public Dimension getPreferredSize() {
		return getMinimumSize();
	}

	public void setSize(Dimension dimension) {
		setSize(dimension.width, dimension.height);
	}

	public void setSize(int width, int height) {
		sourceX = MARGIN;
		cdmX = width - MARGIN - ITEM_WIDTH;

		layoutItems();
		super.setSize(width, height);
	}

	// Set visibility of components based on a search term
	public void filterComponents(String searchTerm, boolean filterTarget) {
		List<LabeledRectangle> components;

		if (filterTarget == true) {
			components = cdmComponents;
			lastTargetFilter = searchTerm;
		} else {
			components = sourceComponents;
			lastSourceFilter = searchTerm;
		}

		for (LabeledRectangle c : components) {
			c.filter(searchTerm);
		}

		layoutItems();
		repaint();
	}

	public void paint(Graphics g) {
		Image offscreen = createVolatileImage(getWidth(), getHeight());
		Graphics2D g2d;

		if (offscreen == null) {
			g2d = (Graphics2D) g;
		} else {
			g2d = (Graphics2D) offscreen.getGraphics();
		}

		g2d.setBackground(Color.WHITE);
		g2d.clearRect(0, 0, getWidth(), getHeight());

		RenderingHints rh = new RenderingHints(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
		g2d.setRenderingHints(rh);

		g2d.setColor(Color.BLACK);
		addLabel(g2d, this.getSourceDbName(), sourceX + ITEM_WIDTH / 2, HEADER_TOP_MARGIN + HEADER_HEIGHT / 2);
		addLabel(g2d, this.getTargetDbName(), cdmX + ITEM_WIDTH / 2, HEADER_TOP_MARGIN + HEADER_HEIGHT / 2);

		if (showingArrowStarts && dragRectangle == null) {
			for (LabeledRectangle item : getVisibleSourceComponents())
				Arrow.drawArrowHead(g2d, Math.round(item.getX() + item.getWidth() + Arrow.headThickness), item.getY() + item.getHeight() / 2);
		}

		for (LabeledRectangle component : getVisibleSourceComponents())
			if (component != dragRectangle)
				component.paint(g2d);

		for (LabeledRectangle component : getVisibleTargetComponents())
			if (component != dragRectangle)
				component.paint(g2d);

		for (int i = HighlightStatus.values().length - 1; i >= 0; i--) {
			HighlightStatus status = HighlightStatus.values()[i];
			for (Arrow arrow : arrowsByStatus(status)) {
				if (arrow != dragArrow) {
					arrow.paint(g2d);
				}
			}
		}

		if (dragRectangle != null)
			dragRectangle.paint(g2d);

		if (dragArrow != null)
			dragArrow.paint(g2d);

		if (offscreen != null)
			g.drawImage(offscreen, 0, 0, this);
	}

	private void addLabel(Graphics2D g2d, String string, int x, int y) {
		g2d.setFont(new Font("default", Font.PLAIN, 20));
		FontMetrics fm = g2d.getFontMetrics();
		Rectangle2D r = fm.getStringBounds(string, g2d);
		g2d.drawString(string, x - Math.round(r.getWidth() / 2), y - Math.round(r.getHeight() / 2) + fm.getAscent());
	}

	@Override
	public void mouseClicked(MouseEvent event) {
		// Save away which arrows are currently highlighted vs normal before we
		// de-select all the tables and arrows
		Hashtable<HighlightStatus, List<Arrow>> currentArrowStatus = new Hashtable<HighlightStatus, List<Arrow>>();
		for (HighlightStatus status : HighlightStatus.values()) {
			currentArrowStatus.put(status, arrowsByStatus(status));
		}

		if (selectedArrow != null) {
			selectedArrow.setSelected(false);
			detailsListener.showDetails(null);
			selectedArrow = null;
		}

		if (!event.isShiftDown() && !((event.getModifiers() & shortcutMask) == shortcutMask)) {
			for (LabeledRectangle component : cdmComponents) {
				component.setSelected(false);
			}

			for (LabeledRectangle component : sourceComponents) {
				component.setSelected(false);
			}
		}
		if (event.getX() > sourceX && event.getX() < sourceX + ITEM_WIDTH) { // Source component
			LabeledRectangleClicked(event, getVisibleSourceComponents());
		} else if (event.getX() > cdmX && event.getX() < cdmX + ITEM_WIDTH) { // target component
			LabeledRectangleClicked(event, getVisibleTargetComponents());
		} else if (event.getX() > sourceX + ITEM_WIDTH && event.getX() < cdmX) { // Arrows
			lastSelectedRectangle = null;
			Arrow clickedArrow = null;
			for (HighlightStatus status : HighlightStatus.values()) {
				for (Arrow arrow : currentArrowStatus.get(status)) {
					if (arrow.contains(event.getPoint())) {
						clickedArrow = arrow;
						break;
					}
				}
				if (clickedArrow != null) {
					break;
				}
			}

			if (clickedArrow != null) {
				if (event.getClickCount() == 2) { // double click
					zoomArrow = clickedArrow;
					if (slaveMappingPanel != null) {
						slaveMappingPanel.setMapping(ObjectExchange.etl.getFieldToFieldMapping((Table) zoomArrow.getSource().getItem(), (Table) zoomArrow
								.getTarget().getItem()));
						new AnimateThread(true).start();

						slaveMappingPanel.filterComponents("", false);
						slaveMappingPanel.filterComponents("", true);
					}

				} else { // single click
					if (!clickedArrow.isSelected()) {
						clickedArrow.setSelected(true);
						selectedArrow = clickedArrow;
						detailsListener.showDetails(mapping.getSourceToTargetMap(selectedArrow.getSource().getItem(), selectedArrow.getTarget().getItem()));
					}
					repaint();
				}
			} else {
				detailsListener.showDetails(null);
			}
		} else {
			lastSelectedRectangle = null;
			detailsListener.showDetails(null);
		}

	}

	private class AnimateThread extends Thread {
		public int		nSteps	= 10;
		private boolean	minimizing;

		public AnimateThread(boolean minimizing) {
			this.minimizing = minimizing;
		}

		public void run() {
			if (minimizing) {
				LabeledRectangle sourceComponent = zoomArrow.getSource();
				LabeledRectangle targetComponent = zoomArrow.getTarget();

				for (LabeledRectangle component : sourceComponents)
					if (component != sourceComponent)
						component.setVisible(false);

				for (LabeledRectangle component : cdmComponents)
					if (component != targetComponent)
						component.setVisible(false);

				for (Arrow component : arrows)
					if (component != zoomArrow)
						component.setVisible(false);
				minimized = true;
				Path heightPath = new Path(getHeight(), HEADER_TOP_MARGIN + HEADER_HEIGHT + MARGIN + ITEM_HEIGHT + BORDER_HEIGHT);
				Path sourcePath = new Path(sourceComponent.getY(), HEADER_TOP_MARGIN + HEADER_HEIGHT);
				Path targetPath = new Path(targetComponent.getY(), HEADER_TOP_MARGIN + HEADER_HEIGHT);
				for (int i = 0; i < nSteps; i++) {
					maxHeight = heightPath.getValue(i);
					sourceComponent.setLocation(sourceX, sourcePath.getValue(i));
					targetComponent.setLocation(cdmX, targetPath.getValue(i));
					for (ResizeListener resizeListener : resizeListeners)
						resizeListener.notifyResized(maxHeight, false, false);
					try {
						sleep(20);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				maxHeight = heightPath.getEnd();
				sourceComponent.setLocation(sourceX, sourcePath.getEnd());
				targetComponent.setLocation(cdmX, targetPath.getEnd());
				for (ResizeListener resizeListener : resizeListeners)
					resizeListener.notifyResized(maxHeight, true, false);
			} else { // maximizing

			}
		}

		private class Path {
			private int	start;
			private int	end;
			private int	stepSize;

			public Path(int start, int end) {
				this.start = start;
				this.end = end;
				this.stepSize = (end - start) / nSteps;
			}

			public int getValue(int step) {
				return start + stepSize * step;
			}

			public int getEnd() {
				return end;
			}
		}
	}

	@Override
	public void mouseEntered(MouseEvent event) {

	}

	@Override
	public void mouseExited(MouseEvent event) {
		if (showingArrowStarts) {
			showingArrowStarts = false;
			repaint();
		}
	}

	private void maximize() {
		maxHeight = Integer.MAX_VALUE;
		minimized = false;
		for (ResizeListener resizeListener : resizeListeners)
			resizeListener.notifyResized(maxHeight, false, true);

		filterComponents(lastSourceFilter, false);

		filterComponents(lastTargetFilter, true);

		for (Arrow component : arrows)
			component.setVisible(true);

		this.requestFocusInWindow();
	}

	@Override
	public void mousePressed(MouseEvent event) {
		if (minimized) {
			maximize();
			return;
		}

		if (event.getX() > sourceX + ITEM_WIDTH && event.getX() < sourceX + ITEM_WIDTH + ARROW_START_WIDTH) { // Arrow starts
			for (LabeledRectangle item : getVisibleSourceComponents()) {
				if (event.getY() >= item.getY() && event.getY() <= item.getY() + item.getHeight()) {
					dragArrow = new Arrow(item);
					dragArrow.setTargetPoint(new Point(item.getX() + item.getWidth() + Arrow.headThickness, item.getY() + item.getHeight() / 2));
					showingArrowStarts = false;
					repaint();
					break;
				}
			}
		} else if (event.getX() > cdmX - ARROW_START_WIDTH && event.getX() < cdmX && dragArrow == null) { // Existing arrowheads
			for (Arrow arrow : arrows) {
				if (event.getY() >= arrow.getTarget().getY() && event.getY() <= arrow.getTarget().getY() + arrow.getTarget().getHeight()
						&& arrow.isSourceAndTargetVisible()) {
					dragArrow = arrow;
					dragArrowPreviousTarget = dragArrow.getTarget();
					dragArrow.setTarget(null);
					break;
				}
			}
			if (dragArrow != null) {
				arrows.remove(dragArrow);
			}
			repaint();
		} else {
			for (LabeledRectangle item : getVisibleSourceComponents()) {
				if (item.contains(event.getPoint()) && !isBeingFiltered()) {
					dragRectangle = item;
					dragOffsetY = event.getY() - item.getY();
					break;
				}
			}

			for (LabeledRectangle item : getVisibleTargetComponents()) {
				if (item.contains(event.getPoint()) && !isBeingFiltered()) {
					dragRectangle = item;
					dragOffsetY = event.getY() - item.getY();
					break;
				}
			}
		}
	}

	@Override
	public void mouseReleased(MouseEvent event) {

		if (dragRectangle != null) { // Dragging rectangles to reorder
			if (!isSorted(sourceComponents, new YComparator())) {
				Collections.sort(sourceComponents, new YComparator());
				mapping.setSourceItems(getItemsList(sourceComponents));
			}
			if (!isSorted(cdmComponents, new YComparator())) {
				Collections.sort(cdmComponents, new YComparator());
				mapping.setTargetItems(getItemsList(cdmComponents));
			}
			dragRectangle = null;
			layoutItems();
		} else if (dragArrow != null) { // dragging arrow to set source and target
			if (event.getX() > cdmX - ARROW_START_WIDTH && event.getX() < cdmX + ITEM_WIDTH)

				for (LabeledRectangle component : getVisibleRectangles(cdmComponents)) {
					if (component.contains(event.getPoint(), ARROW_START_WIDTH, 0)) {
						dragArrow.setTarget(component);
						if (dragArrow.getTarget() == dragArrowPreviousTarget) {
							arrows.add(dragArrow);
							break;
						}

						makeMapSourceToTarget(dragArrow.getSource(), dragArrow.getTarget());
						break;
					}
				}
			if (dragArrowPreviousTarget != null && dragArrow.getTarget() != dragArrowPreviousTarget) { // Retargeted an existing arrow, remove old map from
																										// model
				mapping.removeSourceToTargetMap(dragArrow.getSource().getItem(), dragArrowPreviousTarget.getItem());
			}
			dragArrowPreviousTarget = null;
			dragArrow = null;
		}
		repaint();
	}

	private List<MappableItem> getItemsList(List<LabeledRectangle> components) {
		List<MappableItem> items = new ArrayList<MappableItem>();
		for (LabeledRectangle component : components)
			items.add(component.getItem());
		return items;
	}

	private boolean isSorted(List<LabeledRectangle> sourceComponents2, Comparator<LabeledRectangle> comparator) {
		for (int i = 0; i < sourceComponents2.size() - 1; i++)
			if (comparator.compare(sourceComponents2.get(i), sourceComponents2.get(i + 1)) < 0)
				return false;
		return true;
	}

	@Override
	public void mouseDragged(MouseEvent event) {
		if (dragRectangle != null) {
			dragRectangle.setLocation(dragRectangle.getX(), event.getY() - dragOffsetY);
			layoutItems();
			repaint();
		}
		if (dragArrow != null) {
			if (event.getX() < sourceX + ITEM_WIDTH + Arrow.headThickness)
				dragArrow.setTargetPoint(null);
			else
				dragArrow.setTargetPoint(event.getPoint());
			repaint();
		}
		this.scrollRectToVisible(new Rectangle(event.getX() - 40, event.getY() - 40, 80, 80));

	}

	@Override
	public void mouseMoved(MouseEvent event) {
		if (event.getX() > sourceX + ITEM_WIDTH && event.getX() < sourceX + ITEM_WIDTH + ARROW_START_WIDTH && dragArrow == null) {
			if (!showingArrowStarts) {
				showingArrowStarts = true;
				repaint();
			}
		} else {
			if (showingArrowStarts) {
				showingArrowStarts = false;
				repaint();
			}
		}
	}

	private class YComparator implements Comparator<LabeledRectangle> {

		@Override
		public int compare(LabeledRectangle o1, LabeledRectangle o2) {
			return IntegerComparator.compare(o1.getY(), o2.getY());
		}

	}

	private List<ResizeListener>	resizeListeners	= new ArrayList<ResizeListener>();

	public void addResizeListener(ResizeListener resizeListener) {
		resizeListeners.add(resizeListener);
	}

	public void removeResizeListener(ResizeListener resizeListener) {
		resizeListeners.remove(resizeListener);
	}

	public void setDetailsListener(DetailsListener detailsListener) {
		this.detailsListener = detailsListener;
	}

	private void removeArrow(Arrow a) {
		arrows.remove(a);
		mapping.removeSourceToTargetMap(a.getSource().getItem(), a.getTarget().getItem());
		repaint();
	}

	private List<Arrow> arrowsByStatus(HighlightStatus status) {
		List<Arrow> matchingArrows = new ArrayList<Arrow>();
		for (Arrow arrow : arrows) {
			if (arrow.getHighlightStatus() == status) {
				matchingArrows.add(arrow);
			}
		}
		return matchingArrows;
	}

	private void LabeledRectangleClicked(MouseEvent event, List<LabeledRectangle> components) {
		int startIndex = 0;
		int endIndex = 0;

		for (LabeledRectangle component : components) {

			if (component.contains(event.getPoint())) {

				if ((event.getModifiers() & shortcutMask) == shortcutMask) { // Add one at a time
					component.toggleSelected();
				} else if (event.isShiftDown()) { // Add in consecutive order

					startIndex = Math.min(components.indexOf(lastSelectedRectangle), components.indexOf(component));
					endIndex = Math.max(components.indexOf(lastSelectedRectangle), components.indexOf(component));

					if (startIndex >= 0 && endIndex >= 0) {
						for (int i = startIndex; i <= endIndex; i++) {
							components.get(i).setSelected(true);
						}
					} else {
						component.toggleSelected();
					}

				} else {
					component.setSelected(true);
				}

				if (component.isSelected()) {
					lastSelectedRectangle = component;
				} else {
					lastSelectedRectangle = null;
				}

				detailsListener.showDetails(component.getItem());
				repaint();
				break;
			}
		}
	}

	private List<LabeledRectangle> getSelectedRectangles(List<LabeledRectangle> components) {

		List<LabeledRectangle> selected = new ArrayList<LabeledRectangle>();

		for (LabeledRectangle c : components) {
			if (c.isSelected()) {
				selected.add(c);
			}
		}

		return selected;
	}

	public void makeMapSelectedSourceAndTarget() {

		for (LabeledRectangle source : getSelectedRectangles(sourceComponents)) {
			for (LabeledRectangle target : getSelectedRectangles(cdmComponents)) {
				makeMapSourceToTarget(source, target);
			}
		}

	}

	private void makeMapSourceToTarget(LabeledRectangle source, LabeledRectangle target) {
		boolean isNew = true;

		for (Arrow other : arrows) {
			if (source == other.getSource() && target == other.getTarget()) {
				isNew = false;
			}
		}

		if (isNew) {
			Arrow arrow = new Arrow(source);
			arrow.setTarget(target);
			mapping.addSourceToTargetMap(source.getItem(), target.getItem());
			arrow.setItemToItemMap(mapping.getSourceToTargetMap(source.getItem(), target.getItem()));
			arrows.add(arrow);
		}
		repaint();
	}

	public void removeMapSelectedSourceAndTarget() {

		for (LabeledRectangle source : getSelectedRectangles(sourceComponents)) {
			for (LabeledRectangle target : getSelectedRectangles(cdmComponents)) {
				removeMapSourceToTarget(source, target);
			}
		}
	}

	private void removeMapSourceToTarget(LabeledRectangle source, LabeledRectangle target) {

		for (Iterator<Arrow> iterator = arrows.iterator(); iterator.hasNext();) {
			Arrow arrow = iterator.next();
			if (source == arrow.getSource() && target == arrow.getTarget()) {
				iterator.remove();
			}
		}

		mapping.removeSourceToTargetMap(source.getItem(), target.getItem());
		repaint();
	}

	public boolean isMaximized() {
		return !minimized;
	}

	public List<LabeledRectangle> getVisibleRectangles(List<LabeledRectangle> components) {
		List<LabeledRectangle> visible = new ArrayList<LabeledRectangle>();

		for (LabeledRectangle component : components) {

			if (component.isVisible())
				visible.add(component);
		}

		return visible;
	}

	public String getSourceDbName() {
		String resString = "Source";

		if (this.mapping.getSourceItems().size() > 0) {

			if (this.mapping.getSourceItems().get(0).getDb() != null) {
				resString = this.mapping.getSourceItems().get(0).getDb().getDbName();
			}
		}

		return resString;

	}

	public String getTargetDbName() {
		String resString = "Target";

		if (this.mapping.getTargetItems().size() > 0) {

			if (this.mapping.getTargetItems().get(0).getDb() != null) {
				resString = this.mapping.getTargetItems().get(0).getDb().getDbName();
			}
		}

		return resString;
	}

	public boolean isBeingFiltered() {
		return lastSourceFilter != "" || lastTargetFilter != "";
	}
}

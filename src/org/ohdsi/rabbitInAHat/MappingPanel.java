/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
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
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.swing.JPanel;

import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.Mapping;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.collections.IntegerComparator;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class MappingPanel extends JPanel implements MouseListener, MouseMotionListener, KeyListener {

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
	private List<LabeledRectangle>	targetComponents			= new ArrayList<LabeledRectangle>();
	private List<Arrow>				arrows						= new ArrayList<Arrow>();
	private LabeledRectangle		dragRectangle				= null;
	private LabeledRectangle		selectedRectangle			= null;
	private Arrow					dragArrow					= null;
	private Arrow					zoomArrow					= null;
	private Arrow					selectedArrow				= null;
	private LabeledRectangle		dragArrowPreviousTarget		= null;
	private int						dragOffsetY;
	private int						maxHeight					= Integer.MAX_VALUE;
	private boolean					minimized					= false;
	private MappingPanel			slaveMappingPanel;
	private boolean					showOnlyConnectedItems		= false;

	private boolean					showingArrowStarts			= false;

	private DetailsListener			detailsListener;

	public MappingPanel(Mapping<?> mapping) {
		super();
		this.mapping = mapping;
		this.setFocusable(true);
		addMouseListener(this);
		addMouseMotionListener(this);
		addKeyListener(this);
		renderModel();
	}
	
	public boolean isMinimized(){
		return minimized;
	}
	public void setMapping(Mapping<?> mapping) {
		maximize();
		this.mapping = mapping;
		renderModel();
	}
	
	public List<LabeledRectangle> getVisibleSourceComponents(){
		return getVisibleRectangles(sourceComponents);
	}

	public List<LabeledRectangle> getVisibleTargetComponents(){
		return getVisibleRectangles(targetComponents);
	}
	
	public void setSlaveMappingPanel(MappingPanel mappingPanel) {
		this.slaveMappingPanel = mappingPanel;
	}
	
	public MappingPanel getSlaveMappingPanel(){
		return this.slaveMappingPanel;
	}

	public void setShowOnlyConnectedItems(boolean value) {
		showOnlyConnectedItems = value;
		renderModel();
	}
	
	private void renderModel() {
		sourceComponents.clear();
		targetComponents.clear();
		arrows.clear();
		for (MappableItem item : mapping.getSourceItems())
			if (!showOnlyConnectedItems || isConnected(item)) {
				LabeledRectangle component = new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(255, 128, 0));
				sourceComponents.add(component);
			}
		for (MappableItem item : mapping.getCdmItems())
			if (!showOnlyConnectedItems || isConnected(item)) {
				LabeledRectangle component = new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(128, 128, 255));
				targetComponents.add(component);
			}
		for (ItemToItemMap map : mapping.getSourceToCdmMaps()) {
			Arrow component = new Arrow(getComponentWithItem(map.getSourceItem(), sourceComponents), getComponentWithItem(map.getCdmItem(), targetComponents));
			arrows.add(component);
		}
		layoutItems();
		repaint();
	}

	private boolean isConnected(MappableItem item) {
		for (ItemToItemMap map : mapping.getSourceToCdmMaps())
			if (map.getSourceItem() == item || map.getCdmItem() == item)
				return true;
		return false;
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
			for (LabeledRectangle cdmComponent : getVisibleTargetComponents()){
				cdmComponent.setLocation(cdmX, cdmComponent.getY());
			}
		} else {
			setLabeledRectanglesLocation(getVisibleSourceComponents(),sourceX);
			setLabeledRectanglesLocation(getVisibleTargetComponents(),cdmX);
		}
	}
	
	// Sets the location of the Labeled Rectangles
	private void setLabeledRectanglesLocation(List<LabeledRectangle> components, int xpos){
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
		dimension.height = Math.min(HEADER_HEIGHT + HEADER_TOP_MARGIN + Math.max(sourceComponents.size(), targetComponents.size()) * (ITEM_HEIGHT + MARGIN), maxHeight);
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
	public void filterComponents(String searchTerm, boolean filterTarget){
		List<LabeledRectangle> components;
		
		if( filterTarget == true ){
			components = targetComponents;
		}else{
			components = sourceComponents;
		}
		
		for (LabeledRectangle c : components){	
			c.filter(searchTerm);
		}
		
		layoutItems();
		repaint();
	}
	
	public void paint(Graphics g) {
		Image offscreen = createVolatileImage(getWidth(), getHeight());
		Graphics2D g2d;
		
		if (offscreen == null){
			g2d = (Graphics2D) g;
		}else{
			g2d = (Graphics2D) offscreen.getGraphics();
		}
		
		g2d.setBackground(Color.WHITE);
		g2d.clearRect(0, 0, getWidth(), getHeight());
		
		RenderingHints rh = new RenderingHints(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
		g2d.setRenderingHints(rh); 

		addLabel(g2d, "Source", sourceX + ITEM_WIDTH / 2, HEADER_TOP_MARGIN + HEADER_HEIGHT / 2);
		addLabel(g2d, "CDMv5", cdmX + ITEM_WIDTH / 2, HEADER_TOP_MARGIN + HEADER_HEIGHT / 2);
		
		
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

		for (Arrow component : normalArrows())
			if (component != dragArrow)
				component.paint(g2d);

		if (dragRectangle != null)
			dragRectangle.paint(g2d);

		if (dragArrow != null)
			dragArrow.paint(g2d);
		
		for (Arrow component : highlightedArrows())
			if (component != dragArrow)
				component.paint(g2d);
		
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
		List<Arrow> currentlyHighlightedArrows = highlightedArrows();
		List<Arrow> currentlyNormalArrows = normalArrows();
		
		if (selectedArrow != null) {
			selectedArrow.setSelected(false);
			detailsListener.showDetails(null);
			selectedArrow = null;
		}
		if (selectedRectangle != null) {
			selectedRectangle.setSelected(false);
			detailsListener.showDetails(null);
			selectedRectangle = null;
		}
		
		if (event.getX() > sourceX && event.getX() < sourceX + ITEM_WIDTH) { // Source component
			for (LabeledRectangle component : getVisibleSourceComponents()) {
				if (component.contains(event.getPoint())) {
					if (!component.isSelected()) {
						component.setSelected(true);
						detailsListener.showDetails(component.getItem());
						selectedRectangle = component;
					}
					repaint();
					break;
				}
			}
		}
		if (event.getX() > cdmX && event.getX() < cdmX + ITEM_WIDTH) { // cdm component
			for (LabeledRectangle component : getVisibleTargetComponents()) {
				if (component.contains(event.getPoint())) {
					if (!component.isSelected()) {
						component.setSelected(true);
						detailsListener.showDetails(component.getItem());
						selectedRectangle = component;
					}
					repaint();
					break;
				}
			}
		}
		if (event.getX() > sourceX + ITEM_WIDTH && event.getX() < cdmX) { // Arrows
			Arrow clickedArrow = null;
			
			for (Arrow arrow : currentlyHighlightedArrows) {
				if (arrow.contains(event.getPoint())) {
					clickedArrow = arrow;
					break;
				}
			}
			
			if (clickedArrow == null) {
				for (Arrow arrow : currentlyNormalArrows) {
					if (arrow.contains(event.getPoint())) {
						clickedArrow = arrow;
						break;
					}
				}
			}
			
			if (clickedArrow != null) {
				if (event.getClickCount() == 2) { // double click
					zoomArrow = clickedArrow;
					if (slaveMappingPanel != null) {
						slaveMappingPanel.setMapping(ObjectExchange.etl.getFieldToFieldMapping((Table) zoomArrow.getSource().getItem(), (Table) zoomArrow
								.getTarget().getItem()));
						new AnimateThread(true).start();
					}

				} else { // single click
					if (!clickedArrow.isSelected()) {
						clickedArrow.setSelected(true);
						selectedArrow = clickedArrow;
						detailsListener.showDetails(mapping.getSourceToCdmMap(selectedArrow.getSource().getItem(), selectedArrow.getTarget().getItem()));
					}
					repaint();
				}
			}
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
				LabeledRectangle cdmComponent = zoomArrow.getTarget();

				for (LabeledRectangle component : sourceComponents)
					if (component != sourceComponent)
						component.setVisible(false);

				for (LabeledRectangle component : targetComponents)
					if (component != cdmComponent)
						component.setVisible(false);

				for (Arrow component : arrows)
					if (component != zoomArrow)
						component.setVisible(false);
				minimized = true;
				Path heightPath = new Path(getHeight(), HEADER_TOP_MARGIN + HEADER_HEIGHT + MARGIN + ITEM_HEIGHT + BORDER_HEIGHT);
				Path sourcePath = new Path(sourceComponent.getY(), HEADER_TOP_MARGIN + HEADER_HEIGHT);
				Path cdmPath = new Path(cdmComponent.getY(), HEADER_TOP_MARGIN + HEADER_HEIGHT);
				for (int i = 0; i < nSteps; i++) {
					maxHeight = heightPath.getValue(i);
					sourceComponent.setLocation(sourceX, sourcePath.getValue(i));
					cdmComponent.setLocation(cdmX, cdmPath.getValue(i));
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
				cdmComponent.setLocation(cdmX, cdmPath.getEnd());
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
		for (LabeledRectangle component : sourceComponents)
			component.setVisible(true);

		for (LabeledRectangle component : targetComponents)
			component.setVisible(true);

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
				if (event.getY() >= arrow.getTarget().getY() && event.getY() <= arrow.getTarget().getY() + arrow.getTarget().getHeight() && arrow.isSourceAndTargetVisible()) {
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
				if (item.contains(event.getPoint())) {
					dragRectangle = item;
					dragOffsetY = event.getY() - item.getY();
					break;
				}
			}
			for (LabeledRectangle item : getVisibleTargetComponents()) {
				if (item.contains(event.getPoint())) {
					dragRectangle = item;
					dragOffsetY = event.getY() - item.getY();
					break;
				}
			}
		}
	}

	@Override
	public void mouseReleased(MouseEvent event) {
		if (dragRectangle != null) {
			if (!isSorted(sourceComponents, new YComparator())) {
				Collections.sort(sourceComponents, new YComparator());
				mapping.setSourceItems(getItemsList(sourceComponents));
			}
			if (!isSorted(targetComponents, new YComparator())) {
				Collections.sort(targetComponents, new YComparator());
				mapping.setCdmItems(getItemsList(targetComponents));
			}
			dragRectangle = null;
			layoutItems();
		} else if (dragArrow != null) {
			if (event.getX() > cdmX - ARROW_START_WIDTH && event.getX() < cdmX + ITEM_WIDTH)
				for (LabeledRectangle component : getVisibleRectangles(targetComponents)) {
					if (event.getY() >= component.getY() && event.getY() <= component.getY() + component.getHeight()) {
						dragArrow.setTarget(component);
						if (dragArrow.getTarget() == dragArrowPreviousTarget) {
							arrows.add(dragArrow);
							break;
						}

						boolean isNew = true;
						for (Arrow other : arrows)
							if (dragArrow.getSource() == other.getSource() && dragArrow.getTarget() == other.getTarget())
								isNew = false;
						if (isNew) {
							arrows.add(dragArrow);
							mapping.addSourceToCdmMap(dragArrow.getSource().getItem(), dragArrow.getTarget().getItem());
						}
						break;
					}
				}
			if (dragArrowPreviousTarget != null && dragArrow.getTarget() != dragArrowPreviousTarget) { // Retargeted an existing arrow, remove old map from
																										// model
				mapping.removeSourceToCdmMap(dragArrow.getSource().getItem(), dragArrowPreviousTarget.getItem());
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

	@Override
	public void keyPressed(KeyEvent event) {
		if (event.getKeyCode() == KeyEvent.VK_DELETE) {
			if (selectedArrow != null) {
				arrows.remove(selectedArrow);
				mapping.removeSourceToCdmMap(selectedArrow.getSource().getItem(), selectedArrow.getTarget().getItem());
				repaint();
			}
		}
	}

	@Override
	public void keyReleased(KeyEvent event) {

	}

	@Override
	public void keyTyped(KeyEvent event) {

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
	
	private List<Arrow> highlightedArrows() {
		List<Arrow> highlighted = new ArrayList<Arrow>();
		for(Arrow arrow : arrows) {
			if (arrow.isHighlighted()) {
				highlighted.add(arrow);
			}
		}
		return highlighted;
	}
	
	
	private List<Arrow> normalArrows() {
		List<Arrow> highlighted = new ArrayList<Arrow>();
		for(Arrow arrow : arrows) {
			if (!arrow.isHighlighted()) {
				highlighted.add(arrow);
			}
		}
		return highlighted;
	}

	public List<LabeledRectangle> getVisibleRectangles(List<LabeledRectangle> components){
		List<LabeledRectangle> visible = new ArrayList<LabeledRectangle>();
		
		for( LabeledRectangle component : components){
			
			if(component.isVisible())
			visible.add(component);
		}
		
		return visible;
	}
}

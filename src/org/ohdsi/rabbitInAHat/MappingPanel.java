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

	public void setMapping(Mapping<?> mapping) {
		maximize();
		this.mapping = mapping;
		renderModel();
	}

	public void setSlaveMappingPanel(MappingPanel mappingPanel) {
		this.slaveMappingPanel = mappingPanel;
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
				LabeledRectangle component = new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(255, 128, 0));
				sourceComponents.add(component);
			}
		for (MappableItem item : mapping.getCdmItems())
			if (!showOnlyConnectedItems || isConnected(item)) {
				LabeledRectangle component = new LabeledRectangle(0, 400, ITEM_WIDTH, ITEM_HEIGHT, item, new Color(128, 128, 255));
				cdmComponents.add(component);
			}
		for (ItemToItemMap map : mapping.getSourceToCdmMaps()) {
			Arrow component = new Arrow(getComponentWithItem(map.getSourceItem(), sourceComponents), getComponentWithItem(map.getCdmItem(), cdmComponents));
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

	private void layoutItems() {
		if (minimized) { // Only update x coordinate
			for (LabeledRectangle cdmComponent : cdmComponents)
				cdmComponent.setLocation(cdmX, cdmComponent.getY());
		} else {
			int avoidY = Integer.MAX_VALUE;
			if (dragRectangle != null && dragRectangle.getX() == sourceX)
				avoidY = dragRectangle.getY();
			int y = HEADER_HEIGHT;
			for (int i = 0; i < sourceComponents.size(); i++) {
				LabeledRectangle item = sourceComponents.get(i);
				if (y > avoidY - ITEM_HEIGHT && y <= avoidY + MARGIN)
					y += MARGIN + ITEM_HEIGHT;

				if (dragRectangle == null || item != dragRectangle) {
					item.setLocation(sourceX, y);
					y += MARGIN + ITEM_HEIGHT;
				}
			}

			avoidY = Integer.MAX_VALUE;
			if (dragRectangle != null && dragRectangle.getX() == cdmX)
				avoidY = dragRectangle.getY();
			y = HEADER_HEIGHT;
			for (int i = 0; i < cdmComponents.size(); i++) {
				LabeledRectangle item = cdmComponents.get(i);
				if (y > avoidY - ITEM_HEIGHT && y <= avoidY + MARGIN)
					y += MARGIN + ITEM_HEIGHT;

				if (dragRectangle == null || item != dragRectangle) {
					item.setLocation(cdmX, y);
					y += MARGIN + ITEM_HEIGHT;
				}
			}
		}
	}

	public Dimension getMinimumSize() {
		Dimension dimension = new Dimension();
		dimension.width = 2 * (ITEM_WIDTH + MARGIN) + MIN_SPACE_BETWEEN_COLUMNS;
		dimension.height = Math.min(HEADER_HEIGHT + Math.max(sourceComponents.size(), cdmComponents.size()) * (ITEM_HEIGHT + MARGIN), maxHeight);
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

	public void paint(Graphics g) {
		Image offscreen = createVolatileImage(getWidth(), getHeight());
		Graphics2D g2d;
		if (offscreen == null)
			g2d = (Graphics2D) g;
		else
			g2d = (Graphics2D) offscreen.getGraphics();
		g2d.setBackground(Color.WHITE);
		g2d.clearRect(0, 0, getWidth(), getHeight());
		
		RenderingHints rh = new RenderingHints(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
		g2d.setRenderingHints(rh); 

		addLabel(g2d, "Source", sourceX + ITEM_WIDTH / 2, HEADER_HEIGHT / 2);
		addLabel(g2d, "CDMv5", cdmX + ITEM_WIDTH / 2, HEADER_HEIGHT / 2);

		if (showingArrowStarts && dragRectangle == null) {
			for (LabeledRectangle item : sourceComponents)
				Arrow.drawArrowHead(g2d, Math.round(item.getX() + item.getWidth() + Arrow.headThickness), item.getY() + item.getHeight() / 2);
		}

		for (LabeledRectangle component : sourceComponents)
			if (component != dragRectangle)
				component.paint(g2d);

		for (LabeledRectangle component : cdmComponents)
			if (component != dragRectangle)
				component.paint(g2d);

		for (Arrow component : arrows)
			if (component != dragArrow)
				component.paint(g2d);

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
			for (LabeledRectangle component : sourceComponents) {
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
			for (LabeledRectangle component : cdmComponents) {
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
			for (Arrow component : arrows) {
				if (component.contains(event.getPoint())) {
					if (event.getClickCount() == 2) { // double click
						zoomArrow = component;
						if (slaveMappingPanel != null) {
							slaveMappingPanel.setMapping(ObjectExchange.etl.getFieldToFieldMapping((Table) zoomArrow.getSource().getItem(), (Table) zoomArrow
									.getTarget().getItem()));
							new AnimateThread(true).start();
						}

					} else { // single click
						if (!component.isSelected()) {
							component.setSelected(true);
							selectedArrow = component;
							detailsListener.showDetails(mapping.getSourceToCdmMap(selectedArrow.getSource().getItem(), selectedArrow.getTarget().getItem()));
						}
						repaint();
						break;
					}
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

				for (LabeledRectangle component : cdmComponents)
					if (component != cdmComponent)
						component.setVisible(false);

				for (Arrow component : arrows)
					if (component != zoomArrow)
						component.setVisible(false);
				minimized = true;
				Path heightPath = new Path(getHeight(), HEADER_HEIGHT + MARGIN + ITEM_HEIGHT + BORDER_HEIGHT);
				Path sourcePath = new Path(sourceComponent.getY(), HEADER_HEIGHT);
				Path cdmPath = new Path(cdmComponent.getY(), HEADER_HEIGHT);
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

		for (LabeledRectangle component : cdmComponents)
			component.setVisible(true);

		for (Arrow component : arrows)
			component.setVisible(true);
	}

	@Override
	public void mousePressed(MouseEvent event) {
		if (minimized) {
			maximize();
			return;
		}

		if (event.getX() > sourceX + ITEM_WIDTH && event.getX() < sourceX + ITEM_WIDTH + ARROW_START_WIDTH) { // Arrow starts
			for (LabeledRectangle item : sourceComponents) {
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
				if (event.getY() >= arrow.getTarget().getY() && event.getY() <= arrow.getTarget().getY() + arrow.getTarget().getHeight()) {
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
			for (LabeledRectangle item : sourceComponents) {
				if (item.contains(event.getPoint())) {
					dragRectangle = item;
					dragOffsetY = event.getY() - item.getY();
					break;
				}
			}
			for (LabeledRectangle item : cdmComponents) {
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
			if (!isSorted(cdmComponents, new YComparator())) {
				Collections.sort(cdmComponents, new YComparator());
				mapping.setCdmItems(getItemsList(cdmComponents));
			}
			dragRectangle = null;
			layoutItems();
		} else if (dragArrow != null) {
			if (event.getX() > cdmX - ARROW_START_WIDTH && event.getX() < cdmX + ITEM_WIDTH)
				for (LabeledRectangle component : cdmComponents) {
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

	private boolean isSorted(List<LabeledRectangle> components, Comparator<LabeledRectangle> comparator) {
		for (int i = 0; i < components.size() - 1; i++)
			if (comparator.compare(components.get(i), components.get(i + 1)) < 0)
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

	public void setDetailsListener(DetailsListener detailsListener) {
		this.detailsListener = detailsListener;
	}

}

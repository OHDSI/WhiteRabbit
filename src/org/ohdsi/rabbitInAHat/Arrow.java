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

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Polygon;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;

public class Arrow implements MappingComponent {

	public enum HighlightStatus {
		IS_SELECTED (new Color(204, 255, 204, 192)),
		BOTH_SELECTED (new Color(255, 255, 0, 192)),
		SOURCE_SELECTED (new Color(255, 128, 0, 192)),
		TARGET_SELECTED (new Color(0, 0, 255, 192)),
		NONE_SELECTED (new Color(128, 128, 128, 192)),
		IS_COMPLETED (new Color(128, 128, 128, 50));
		
		private final Color color;
		
		HighlightStatus(Color color) {
			this.color = color;
		}
	}
	
	public static float			thickness		= 5;
	public static int			headThickness	= 15;
	public static Color			color	    	= HighlightStatus.NONE_SELECTED.color;
	private static BasicStroke	dashed			= new BasicStroke(2.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, new float[] { 10.f }, 0.0f);

	private int					x1;
	private int					y1;
	private int					x2;
	private int					y2;
	private LabeledRectangle	source			= null;
	private LabeledRectangle	target			= null;
	private ItemToItemMap		itemToItemMap;

	private int					width;
	private int					height;

	private Polygon				polygon;

	private boolean				isSelected		= false;
	private boolean				isVisible		= true;
	
	public Arrow(LabeledRectangle source) {
		this.source = source;
	}
	
	public Arrow(int x1, int y1, int x2, int y2) {
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;

		this.width = Math.abs(x1 - x2);
		this.height = Math.abs(y1 - y2);
	}

	public Arrow(LabeledRectangle source, LabeledRectangle target) {
		this.source = source;
		this.target = target;
	}
	
	public Arrow(LabeledRectangle source, LabeledRectangle target, ItemToItemMap itemToItemMap) {
		this.source = source;
		this.target = target;	
		this.itemToItemMap = itemToItemMap;
	}

	public ItemToItemMap getItemToItemMap() {
		return itemToItemMap;
	}
	
	public void setItemToItemMap(ItemToItemMap itemToItemMap) {
		this.itemToItemMap = itemToItemMap;
	}
	
	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public boolean isVisible(){
		return isVisible;
	}
	public LabeledRectangle getSource() {
		return source;
	}

	public void setTargetPoint(Point point) {
		if (point == null) {
			x2 = source.getX() + source.getWidth() + Arrow.headThickness;
			y2 = source.getY() + source.getHeight() / 2;
		} else {
			x2 = point.x;
			y2 = point.y;
		}
	}

	public void paint(Graphics g) {
		if (!isVisible)
			return;
		
		if( source != null && target != null){
			if(!source.isVisible() || !target.isVisible()){
				return;
			}
		}
		Graphics2D g2d = (Graphics2D) g;

		if (source != null) {
			x1 = source.getX() + source.getWidth();
			y1 = source.getY() + source.getHeight() / 2;
			width = Math.abs(x1 - x2);
			height = Math.abs(y1 - y2);
		}
		if (target != null) {
			x2 = target.getX();
			y2 = target.getY() + target.getHeight() / 2;
			width = Math.abs(x1 - x2);
			height = Math.abs(y1 - y2);
		}
		int nPoints = 25;
		int[] xPoints = new int[nPoints * 2 + 3];
		int[] yPoints = new int[nPoints * 2 + 3];
		float widthMinHead = getWidth() - headThickness;
		float stepSize = widthMinHead / (float) (nPoints - 1);

		for (int i = 0; i < nPoints; i++) {
			float x = x1 + stepSize * i;
			float y = (float) (y1 + (Math.cos(Math.PI * i / (float) nPoints) / 2d - 0.5) * (y1 - y2));
			xPoints[i] = Math.round(x);
			yPoints[i] = Math.round(y - thickness);
			xPoints[nPoints * 2 + 3 - i - 1] = Math.round(x);
			yPoints[nPoints * 2 + 3 - i - 1] = Math.round(y + thickness);
		}
		xPoints[nPoints] = x2 - headThickness;
		yPoints[nPoints] = y2 - headThickness;
		xPoints[nPoints + 1] = x2;
		yPoints[nPoints + 1] = y2;
		xPoints[nPoints + 2] = x2 - headThickness;
		yPoints[nPoints + 2] = y2 + headThickness;
		polygon = new Polygon(xPoints, yPoints, nPoints * 2 + 3);

		g2d.setColor(fillColor());
		g2d.fillPolygon(polygon);

		if (isSelected) {
			g2d.setColor(Color.BLACK);
			g2d.setStroke(dashed);
			g2d.drawPolygon(polygon);
		}
	}

	public Color fillColor() {
		return getHighlightStatus().color;
	}

	private boolean isTargetSelected() {
		return target != null && target.isSelected();
	}

	private boolean isSourceSelected() {
		return source != null && source.isSelected();
	}

	public static void drawArrowHead(Graphics2D g2d, int x, int y) {
		int nPoints = 3;
		int[] xPoints = new int[nPoints];
		int[] yPoints = new int[nPoints];
		xPoints[0] = x - headThickness;
		yPoints[0] = y - headThickness;
		xPoints[1] = x;
		yPoints[1] = y;
		xPoints[2] = x - headThickness;
		yPoints[2] = y + headThickness;
		g2d.setColor(color);
		g2d.fillPolygon(xPoints, yPoints, nPoints);
	}

	public void setTarget(LabeledRectangle target) {
		this.target = target;
	}

	public LabeledRectangle getTarget() {
		return target;
	}

	public HighlightStatus getHighlightStatus() {
		if (isSelected()) {
			return HighlightStatus.IS_SELECTED;
		} else if (isSourceSelected() && isTargetSelected()) {
			return HighlightStatus.BOTH_SELECTED;
		} else if (isSourceSelected()) {
			return HighlightStatus.SOURCE_SELECTED;
		} else if (isTargetSelected()) {
			return HighlightStatus.TARGET_SELECTED;
		} else if (isCompleted()) {
			return HighlightStatus.IS_COMPLETED;
		} else {
			return HighlightStatus.NONE_SELECTED;
		}
	}

	public boolean isSelected() {
		return isSelected;
	}
	
	public void setSelected(boolean isSelected) {
		this.isSelected = isSelected;
	}

	public boolean isCompleted() {
		if (getItemToItemMap() != null) {
			return getItemToItemMap().isCompleted();
		}
		else {
			return false;
		}
	}
	
	public boolean contains(Point point) {
		return polygon.contains(point);
	}

	public void setVisible(boolean value) {
		isVisible = value;
	}
	
	public boolean isSourceAndTargetVisible(){
		return source.isVisible() && target.isVisible();
	}
	
	public boolean isConnected(){
		return source != null && target != null;
	}
}

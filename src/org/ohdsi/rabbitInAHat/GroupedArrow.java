package org.ohdsi.rabbitInAHat;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.util.ArrayList;
import java.util.List;

import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;

public class GroupedArrow extends Arrow{
	
	private List<LabeledRectangle> sources = new ArrayList<LabeledRectangle>();
	private List<ItemToItemMap> maps = new ArrayList<ItemToItemMap>();
	private boolean isVisible = true;
	private int numOfArrows;
	
	public GroupedArrow(LabeledRectangle source, LabeledRectangle target, List<LabeledRectangle> s) {
		super(null,target);
		this.sources = s;
		this.numOfArrows = sources.size();
	}
	
	public GroupedArrow(LabeledRectangle source, LabeledRectangle target, ItemToItemMap itemToItemMap, List<ItemToItemMap> listOfMaps) {
		super(null, target, null);
		this.maps = listOfMaps;
	}
	
	public List<LabeledRectangle> getList(){
		return sources;
	}
	
	public List<ItemToItemMap> getMapList(){
		return maps;
	}
	public void setGroup(List<LabeledRectangle> s){
		this.sources = s;
	}
	
	//TODO Alter to accommodate for multiple sources and one target
	public void paint(Graphics g) {
		if(!isVisible)
			return;
		
		if(getList() != null && getTarget() != null){
			if(isVisible() || !getTarget().isVisible()){
				return;
			}
		}
		Graphics2D g2d = (Graphics2D) g;
		for (LabeledRectangle source : sources){		
		//TODO make for GroupedArrow
			if (source != null) {
				x1 = source.getX() + source.getWidth();
				y1 = source.getY() + source.getHeight() / 2;
				width = Math.abs(x1 - x2);
				height = Math.abs(y1 - y2);
			}
			if (getTarget() != null) {
				x2 = getTarget().getX();
				y2 = getTarget().getY() + getTarget().getHeight() / 2;
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
	
			if (getIsSelected()) {
				g2d.setColor(Color.BLACK);
				g2d.setStroke(dashed);
				g2d.drawPolygon(polygon);
			}
		}
	}
	
	public void addItemToItemMap(ItemToItemMap itemToItemMap) {
		maps.add(itemToItemMap);
	}
	
	public void addItemToItemMap(List<ItemToItemMap> maps){
		this.maps = maps;
	}
	
	public int getNumOfArrows(){
		return numOfArrows;
	}
}
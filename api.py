from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, JSON, TIMESTAMP, ForeignKey, false
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import TEXT
import requests
import uuid
import json
import logging
import re
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Supabase database connection
db_url = 'postgresql://postgres.updemyybuhoicisjksiu:Dexter_#254@aws-0-eu-central-1.pooler.supabase.com:6543/postgres'
try:
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
except Exception as e:
    logger.error(f"Failed to connect to Supabase: {str(e)}")
    exit(1)

# SQLAlchemy base
Base = declarative_base()

# Define models (same as provided)
class Place(Base):
    __tablename__ = 'places'
    place_id = Column(TEXT, primary_key=True)
    name = Column(TEXT)
    description = Column(TEXT)
    website = Column(TEXT)
    phone = Column(TEXT)
    is_spending_on_ads = Column(Boolean)
    rating = Column(Float)
    reviews_count = Column(Integer)
    main_category = Column(TEXT)
    workday_timing = Column(TEXT)
    is_temporarily_closed = Column(Boolean)
    is_permanently_closed = Column(Boolean)
    address = Column(TEXT)
    plus_code = Column(TEXT)
    link = Column(TEXT)
    status = Column(TEXT)
    price_range = Column(TEXT)
    reviews_link = Column(TEXT)
    time_zone = Column(TEXT)
    latitude = Column(Float)
    longitude = Column(Float)
    cid = Column(TEXT)
    data_id = Column(TEXT)

class RawPlaceData(Base):
    __tablename__ = 'raw_place_data'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    raw_data = Column(JSON)

class Owner(Base):
    __tablename__ = 'owners'
    gmaps_id = Column(TEXT, primary_key=True)
    name = Column(TEXT)
    link = Column(TEXT)

class PlaceOwner(Base):
    __tablename__ = 'place_owners'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    owner_id = Column(TEXT, ForeignKey('owners.gmaps_id'), primary_key=True)

class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(TEXT, unique=True)

class PlaceCategory(Base):
    __tablename__ = 'place_categories'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    category_id = Column(Integer, ForeignKey('categories.id'), primary_key=True)

class Hour(Base):
    __tablename__ = 'hours'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    day = Column(TEXT)
    open_time = Column(TEXT)
    close_time = Column(TEXT)

class DetailedAddress(Base):
    __tablename__ = 'detailed_address'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    street = Column(TEXT)
    city = Column(TEXT)
    state = Column(TEXT)
    postal_code = Column(TEXT)
    country_code = Column(TEXT)

class Review(Base):
    __tablename__ = 'reviews'
    review_id = Column(TEXT, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    rating = Column(Integer)
    name = Column(TEXT)
    reviewer_profile = Column(TEXT)
    review_text = Column(TEXT)
    published_at = Column(TIMESTAMP)
    response_from_owner_text = Column(TEXT)
    response_from_owner_date = Column(TIMESTAMP)

class FeaturedQuestion(Base):
    __tablename__ = 'featured_questions'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    question = Column(TEXT)
    answer = Column(TEXT)
    question_date = Column(TIMESTAMP)
    answer_date = Column(TIMESTAMP)
    asked_by_name = Column(TEXT)
    answered_by_name = Column(TEXT)
    answered_by_link = Column(TEXT)

class ReviewKeyword(Base):
    __tablename__ = 'review_keywords'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    keyword = Column(TEXT)
    count = Column(Integer)

class Image(Base):
    __tablename__ = 'images'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    about = Column(TEXT)
    link = Column(TEXT)

class About(Base):
    __tablename__ = 'about'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    section_id = Column(TEXT)
    section_name = Column(TEXT)

class AboutOption(Base):
    __tablename__ = 'about_options'
    id = Column(Integer, primary_key=True)
    about_id = Column(Integer, ForeignKey('about.id'))
    name = Column(TEXT)
    enabled = Column(Boolean)

# Create tables
Base.metadata.create_all(engine)

# FastAPI app
app = FastAPI(debug=False)

# Request model
class SearchRequest(BaseModel):
    queries: List[str]
    api_key: str
    coordinates: Optional[str] = "40.6970194,-74.3093048"
    zoom_level: Optional[int] = 11
    lang: Optional[str] = "en"
    region: Optional[str] = "us"
    max_results: Optional[int] = None
    enable_reviews_extraction: Optional[bool] = False
    max_reviews: Optional[int] = None
    reviews_sort: Optional[str] = "newest"

def search_google_maps(query: str, page: int, location: str, language: str, region: str, extra: bool, api_token: str):
    logger.info("Calling GmapsExtractor API for query: %s, page: %d", query, page)
    url = "https://cloud.gmapsextractor.com/api/v2/search"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    payload = {
        "q": query,
        "page": page,
        "ll": location,
        "hl": language,
        "gl": region,
        "extra": extra
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info("API call successful for query: %s", query)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        return None
    except requests.exceptions.RequestException as err:
        logger.error(f"Error occurred: {err}")
        return None

@app.post("/api/search")
async def search_places(search_request: SearchRequest):
    session = Session()
    try:
        # Extract input data
        queries = search_request.queries
        api_key = search_request.api_key
        coordinates = search_request.coordinates
        zoom_level = search_request.zoom_level
        lang = search_request.lang
        region = search_request.region
        max_results = search_request.max_results
        enable_reviews_extraction = search_request.enable_reviews_extraction
        max_reviews = search_request.max_reviews
        reviews_sort = search_request.reviews_sort

        # Validate inputs
        if not queries:
            raise HTTPException(status_code=400, detail="No search queries provided")
        if not api_key:
            raise HTTPException(status_code=400, detail="API key is required")

        # Format location
        try:
            coords = coordinates.strip().lstrip('@')
            location = f"@{coords},{zoom_level}z"
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid coordinates or zoom level: {str(e)}")

        all_results = []
        for query in queries:
            logger.info(f"Processing query: {query}")
            
            # Call the API
            result = search_google_maps(
                query=query,
                page=1,
                location=location,
                language=lang,
                region=region,
                extra=True,
                api_token=api_key
            )
            
            if not result or "data" not in result:
                logger.error(f"No results found or API error for query '{query}'")
                all_results.append({
                    "query": query,
                    "error": "No results found or API error",
                    "status": "failed"
                })
                continue

            # Filter results
            places = result.get("data", [])
            if max_results:
                places = places[:max_results]

            query_results = {
                "query": query,
                "total": result.get("total", 0),
                "places": [],
                "status": "running"
            }

            for place in places:
                place_id = place.get("place_id")
                lat = place.get("latitude")
                lon = place.get("longitude")
                is_spending_on_ads = place.get("tracking_ids", {}).get("google", {}).get("ads") is not None

                # Store place
                place_record = Place(
                    place_id=place_id,
                    name=place.get("name"),
                    description=place.get("meta", {}).get("description"),
                    website=place.get("website"),
                    phone=place.get("phone"),
                    is_spending_on_ads=is_spending_on_ads,
                    rating=place.get("average_rating"),
                    reviews_count=place.get("review_count"),
                    main_category=place.get("categories"),
                    workday_timing=place.get("opening_hours"),
                    is_temporarily_closed=place.get("is_temporarily_closed", False),
                    is_permanently_closed=place.get("is_permanently_closed", False),
                    address=place.get("full_address"),
                    plus_code=place.get("plus_code"),
                    link=place.get("google_maps_url"),
                    status=place.get("status"),
                    price_range=place.get("price_range"),
                    reviews_link=place.get("review_url"),
                    time_zone=place.get("time_zone"),
                    latitude=lat,
                    longitude=lon,
                    cid=place.get("cid"),
                    data_id=place.get("data_id")
                )
                session.add(place_record)

                # Store raw JSON data
                raw_place_data = RawPlaceData(
                    place_id=place_id,
                    raw_data=place
                )
                session.add(raw_place_data)

                # Flush Place
                try:
                    session.flush()
                    logger.info(f"Flushed place {place_id}")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error flushing place {place_id}: {str(e)}")
                    continue

                # Store detailed address
                addr_parts = place.get("full_address", "").split(", ")
                detailed_address = DetailedAddress(
                    place_id=place_id,
                    street=addr_parts[0] if len(addr_parts) > 0 else None,
                    city=addr_parts[1] if len(addr_parts) > 1 else None,
                    state=addr_parts[2] if len(addr_parts) > 2 else None,
                    postal_code=addr_parts[3] if len(addr_parts) > 3 else None,
                    country_code=addr_parts[4] if len(addr_parts) > 4 else None
                )
                session.add(detailed_address)

                # Store categories
                categories = place.get("categories", "").split(", ") if place.get("categories") else []
                with session.no_autoflush:
                    for cat_name in categories:
                        if cat_name:
                            category = session.query(Category).filter_by(name=cat_name).first()
                            if not category:
                                category = Category(name=cat_name)
                                session.add(category)
                                session.flush()
                                logger.info(f"Added new category: {cat_name}")
                            place_category = PlaceCategory(
                                place_id=place_id,
                                category_id=category.id
                            )
                            session.add(place_category)

                # Store reviews metadata
                if enable_reviews_extraction:
                    review_id = str(uuid.uuid4())
                    reviews_info = {
                        "enabled": enable_reviews_extraction,
                        "max_reviews": max_reviews,
                        "sort": reviews_sort,
                        "note": "Reviews extraction not supported by this API"
                    }
                    review = Review(
                        review_id=review_id,
                        place_id=place_id,
                        rating=None,
                        name=None,
                        reviewer_profile=None,
                        review_text=json.dumps(reviews_info),
                        published_at=None,
                        response_from_owner_text=None,
                        response_from_owner_date=None
                    )
                    session.add(review)

                # Store about
                if categories:
                    about_record = About(
                        place_id=place_id,
                        section_id=str(uuid.uuid4()),
                        section_name="Categories"
                    )
                    session.add(about_record)

                # Store images
                featured_image = place.get("featured_image")
                if featured_image:
                    image_record = Image(
                        place_id=place_id,
                        about=place.get("categories"),
                        link=featured_image
                    )
                    session.add(image_record)

                # Store reviews
                if place.get("average_rating") or place.get("name") or place.get("review_url"):
                    review_id = str(uuid.uuid4())
                    review_record = Review(
                        review_id=review_id,
                        place_id=place_id,
                        rating=int(place.get("average_rating", 0)) if place.get("average_rating") else None,
                        name=place.get("name"),
                        reviewer_profile=place.get("review_url"),
                        review_text=None,
                        published_at=None,
                        response_from_owner_text=None,
                        response_from_owner_date=None
                    )
                    session.add(review_record)

                # Parse and store hours
                opening_hours = place.get("opening_hours")
                if opening_hours:
                    hours_entries = re.findall(r"(\w+):\s*\[(.*?)\]", opening_hours)
                    for day, hours in hours_entries:
                        if hours.lower() == "closed":
                            hour_record = Hour(
                                place_id=place_id,
                                day=day,
                                open_time=None,
                                close_time="Closed"
                            )
                        else:
                            times = hours.split("-")
                            if len(times) == 2:
                                open_time, close_time = times
                                hour_record = Hour(
                                    place_id=place_id,
                                    day=day,
                                    open_time=open_time.strip(),
                                    close_time=close_time.strip()
                                )
                            else:
                                continue
                        session.add(hour_record)

                query_results["places"].append(place)

            # Commit transaction
            try:
                session.commit()
                logger.info(f"Successfully stored {len(places)} places for query '{query}'")
                query_results["places_stored"] = len(places)
                query_results["status"] = "completed"
            except Exception as e:
                session.rollback()
                logger.error(f"Error storing places for query '{query}': {str(e)}")
                query_results["error"] = f"Database error: {str(e)}"
                query_results["status"] = "failed"

            all_results.append(query_results)

        final_result = {
            "results": all_results,
            "status": "completed"
        }
        logger.info("API request completed with result: %s", json.dumps(final_result, indent=2))
        return final_result

    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    finally:
        session.close()
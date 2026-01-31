#https://www.youtube.com/watch?v=7EeAZx78P2U&list=PLlLKnYbrXi_rFrzsPa0NxZayHrT52a777&index=6

#from itsdangerous import TimedJSONWebSignatureSerializer as Serializer  #pip install itsdangerous==2.0.1
from itsdangerous.url_safe import URLSafeTimedSerializer as Serializer
#from mywebpage import db, app
#from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, func
#from sqlalchemy.orm import relationship, backref
from mywebpage.db import async_session_scope
from sqlalchemy.future import select
from sqlalchemy import Text, text, Column,UnicodeText, Boolean, JSON, DateTime, String, Float, Integer, NVARCHAR, ForeignKey, CheckConstraint, Unicode
import json
from sqlalchemy.orm import declarative_base
from contextlib import contextmanager
from sqlalchemy import create_engine, JSON
from sqlalchemy.orm import sessionmaker, scoped_session
from pytz import timezone as pytz_timezone, UTC
from dateutil import parser
import pytz

# Create the declarative base
Base = declarative_base()

#from sqlalchemy import Column, Integer, String, ForeignKey, Text, DateTime, CheckConstraint
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy import Index
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB, ARRAY





async def update_client_mode(org_id, mode):
    if mode not in ['automatic', 'manual']:
        raise ValueError("Invalid mode. Must be 'automatic' or 'manual'.")

    async with async_session_scope() as session:
        client = await session.scalar(
            select(Client).where(Client.id == org_id)
        )

        if not client:
            print(f"Client with id '{org_id}' not found.")
            return

        client.mode = mode
        print(f"Updated mode for client '{org_id}' to '{mode}'.")






class Subscription(Base):
    __tablename__ = 'subscriptions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Unicode(50), unique=True, nullable=False)
    description = Column(Unicode(255))
    can_access_chat_control = Column(Boolean, default=False)
    can_access_basic_metrics = Column(Boolean, default=False)        # weekly summary
    can_access_enhanced_metrics = Column(Boolean, default=False)     # hierarchical charts / advanced analysis
    can_access_advanced_ai = Column(Boolean, default=False)
    base_seats = Column(Integer, nullable=False)


    # Stripe integration
    stripe_product_id = Column(String(255), nullable=True)  # Stripe Product ID pl prod_NJ2fXk9kL5wQd
    stripe_price_id = Column(String(255), nullable=True)    # Stripe Price ID (monthly) pl.price_1N1k4L2aCzXyA7 means: Basic: $50/month

    # Optional usage quotas
    max_chats_per_month = Column(Integer, default=0)


    # Relationships
    clients = relationship("Client", back_populates="subscription")


# it tartom nyilván, hogy mik éppen az aktív árak, de megőrzi a régebbi árakat is
class SubscriptionPrice(Base):
    __tablename__ = "subscription_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    subscription_id = Column(Integer, ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False)
    stripe_price_id = Column(String(255), nullable=False)   # Stripe Price ID
    amount = Column(Integer, nullable=False)               # price in minor units (e.g., HUF cents)
    currency = Column(String(10), default="HUF", nullable=False)
    active = Column(Boolean, default=True)                 # only one active per subscription at a time
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    archived_at = Column(DateTime(timezone=True), nullable=True)

    subscription = relationship("Subscription", backref="prices")


class Client(Base):
    __tablename__ = 'clients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_name = Column(UnicodeText, nullable=False)
    client_code = Column(Unicode(50), unique=True, nullable=False)
    api_key = Column(Unicode(255), nullable=False)
    subscription_id = Column(Integer, ForeignKey('subscriptions.id', ondelete='SET NULL'), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    context = Column(UnicodeText)
    mode = Column(Unicode(20), nullable=False, default='automatic')
    last_manualmode_triggered_by = Column(Unicode(255), nullable=True)
    timezone = Column(Unicode(100), default='UTC') 
    billing_email = Column(String(255), nullable=True)

    icon_path = Column(UnicodeText, nullable=True)       # static URL path to icon
    use_google_icon = Column(Boolean, default=True)      # fallback if no custom icon
    agent_icon = Column(Boolean, nullable=False, default=True)
    emoji_icon = Column(Boolean, nullable=False, default=True)
    attachment_icon = Column(Boolean, nullable=False, default=True)
    auto_upgrade = Column(Boolean, default=False, nullable=False)
    seats = Column(Integer, nullable=False, default=0)


    languages = Column(ARRAY(String), nullable=False, server_default=text('ARRAY["hu"]'))
    
    primary_color = Column(Text)  # Accepts gradients like 'linear-gradient(...)'
    border_radius = Column(Unicode(20), nullable=True)  # e.g., '40px'
    border_width = Column(Unicode(10), nullable=True)   # e.g., '1px'
    border_color = Column(Unicode(50), nullable=True)   # e.g., 'rgb(193, 192, 192)'
    reply_bg_color = Column(Text)
    operator_icon=Column(String(50), nullable=True)
    font_color = Column(Text)
    font_header_text = Column(Unicode(100), nullable=False, server_default='"Inter", sans-serif')
    header_font_weight=Column(Unicode(20))
    header_font_size = Column(Unicode(10), nullable=False, server_default=text('"1.1rem"'))
    font_general = Column(Unicode(100), nullable=False, server_default='"Inter", sans-serif')
    general_body_font_size = Column(Unicode(10), nullable=False, server_default=text('"12.5px"'))
    general_body_font_size2 = Column(Unicode(10), nullable=False, server_default=text('"11px"'))


    everything_which_is_white = Column(Text)
    user_input_message_color = Column(Text)
    popup_bg_color = Column(Text)
    footer_bg_color = Column(Text)
    footer_controls_bg = Column(Text)
    footer_input_bg_color = Column(Text)
    footer_focus_outline_color = Column(Text)
    scrollbar_color = Column(Text)
    confirmation_button_bgcolor = Column(Text)

    config_updated_by = Column(String(255), nullable=True)
    config_updated_at = Column(DateTime(timezone=True), nullable=True)
    language_hu_logo_text = Column(String, default="Automata és élő ügyintézés")
    language_en_logo_text = Column(String, default="AI & Live Support")
    language_selector=Column(String, default="Please select your language / Kérlek válassz nyelvet:")
    greeting_message_hu=Column(String, default="Üdvözöllek! Miben tudok segíteni?")
    greeting_message_en=Column(String, default="Welcome! How can I help you?")
    agent_request_confirmation_hu=Column(String, default="Szeretne ügyintézővel beszélni?")
    agent_request_confirmation_en=Column(String, default="Would you like to talk to a colleague?")
    textfont_updated_by = Column(String(255), nullable=True)
    textfont_updated_at = Column(DateTime(timezone=True), nullable=True)
    
    # Stripe integration
    stripe_customer_id = Column(String(255), nullable=True)   #unique ID that Stripe assigns to my customer, client in my system that wants to pay or subscribe gets a Stripe customer.
    stripe_subscription_id = Column(String(255), nullable=True)  # ID of the active subscription in Stripe for this customer, To know which subscription tier the client has (Basic / Gold / Platinum)
    subscription_start_date = Column(DateTime(timezone=True), nullable=True)
    subscription_end_date = Column(DateTime(timezone=True), nullable=True)



    
    __table_args__ = (
        CheckConstraint("mode IN ('automatic', 'manual')", name='check_mode'),
    )

    # Relationships
    subscription = relationship("Subscription", back_populates="clients")
    users = relationship("User", back_populates="client", cascade="all, delete-orphan")
    chat_messages = relationship("ChatHistory", back_populates="client", cascade="all, delete-orphan")
    mode_overrides = relationship("UserModeOverride", back_populates="client", cascade="all, delete-orphan")
    connections = relationship("Connections", back_populates="org", cascade="all, delete-orphan")
    
    config_history = relationship("ClientConfigHistory", back_populates="client", cascade="all, delete-orphan", order_by="desc(ClientConfigHistory.created_at)")
    behavior_history = relationship("ClientBehaviorHistory", back_populates="client", cascade="all, delete-orphan", order_by="desc(ClientConfigHistory.created_at)")


# DESIGN PARAMS SAVE
class ClientConfigHistory(Base):
    __tablename__ = "client_config_history"

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"))
    parameters = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship back to Client
    client = relationship("Client", back_populates="config_history")


#FONT OTHER STRUCTURAL PARAMS SAVE
class ClientBehaviorHistory(Base):
    __tablename__ = "client_behavior_history"

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"))
    parameters = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship back to Client
    client = relationship("Client", back_populates="behavior_history")


class ChatHistory(Base):
    __tablename__ = 'chat_messages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    client_id = Column(Integer, ForeignKey('clients.id'), nullable=True)
    user_id = Column(Unicode(36))  # User ID, can be alphanumeric
    message = Column(UnicodeText)  # Use UnicodeText for large text fields
    response = Column(UnicodeText)  # Use UnicodeText for large text fields
    topic = Column(UnicodeText)  # Store topic as Unicode (255 characters should be sufficient)
    topic_classification = Column(UnicodeText)
    latitude = Column(Float)  # Latitude for geolocation
    longitude = Column(Float)  # Longitude for geolocation
    location = Column(Unicode(255))  # Store location as Unicode (255 characters)
    # wordtext_para = Column(UnicodeText)  # Use UnicodeText for word-related data
    # extracted_relevant_paragraphs = Column(UnicodeText)  # Use UnicodeText for paragraphs, stored as JSON
    # dynamic_txt=Column(UnicodeText)
    # client_details_placeholder = Column(UnicodeText)  # Use UnicodeText for client details placeholder
    # context = Column(UnicodeText)  # Use UnicodeText for context, stored as JSON
    mode = Column(Unicode(10)) 
    agent = Column(Unicode(255))

    

    # Define relationships
    client = relationship("Client", back_populates="chat_messages")

    def __repr__(self):
        return f"<ChatHistory(id={self.id}, created_at={self.created_at}, client_id={self.client_id}, user_id={self.user_id}, message={self.message})>"

  


class Role(Base):
    __tablename__ = 'roles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    role_name = Column(Unicode(50), unique=True, nullable=False)

    def __repr__(self):
        return f'<Role {self.role_name}>'



class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)   #!!!!! ADMINS
    client_id = Column(Integer, ForeignKey('clients.id', ondelete='CASCADE'), nullable=False)
    email = Column(Unicode(50), unique=True, nullable=False)
    name = Column(Unicode(100), nullable=True)
    is_active = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    role_id = Column(Integer, ForeignKey('roles.id'))
    is_deleted = Column(Boolean, default=False, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    # admin_internal_message_open = Column(DateTime(timezone=True), nullable=True)
    # admin_internal_message_close = Column(DateTime(timezone=True), nullable=True)

    language = Column(String(5), default='hu', nullable=False) 
    
    # Relationships
    role = relationship("Role", backref="users")
    client = relationship("Client", back_populates="users")

    def soft_delete(self):
        self.is_deleted = True
        self.deleted_at = datetime.utcnow()

    def restore(self):
        self.is_deleted = False
        self.deleted_at = None

    __table_args__ = (
        Index('ix_users_client_id_is_deleted', 'client_id', 'is_deleted'),
    )



#  ------------ These are moved to REDIS: ----------------------



class UserModeOverride(Base):
    __tablename__ = 'user_mode_overrides'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Unicode(36), nullable=False)   #   !!! POPUP USERS
    client_id = Column(Integer, ForeignKey('clients.id', ondelete='CASCADE'), nullable=False)
    mode = Column(Unicode(20), nullable=False, default='manual')  # Use Unicode and limit length
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    client = relationship("Client", back_populates="mode_overrides")


class Connections(Base):
    __tablename__ = 'connections'

    # Columns
    socket_id = Column(String(255), primary_key=True)  # Use String with length limit
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)  # ADMIN !!!!
    org_id = Column(Integer, ForeignKey('clients.id'), nullable=False)
    manualmode_triggered = Column(Boolean, default=False, nullable=False)
    disconnected_at = Column(DateTime(timezone=True), nullable=True)

    # Relationship only with Client (retain this)
    org = relationship("Client", back_populates="connections")

    def __repr__(self):
        return f"<Connections(socket_id='{self.socket_id}', user_id={self.user_id}, org_id={self.org_id})>"



# ------------------------------------------------------------



class OrgEventLog(Base):   #save all the events, messages made on the admin page
    __tablename__ = 'org_event_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, ForeignKey('clients.id'), nullable=False)
    event_type = Column(String(255), nullable=False)
    data = Column(JSONB, nullable=False)  # Storing event-specific data
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)  

    __table_args__ = (
        Index('ix_org_event_logs_org_event_timestamp', 'org_id', 'event_type', 'timestamp'),
        Index('ix_org_event_logs_org_timestamp', 'org_id', 'timestamp'),
    )




def convert_utc_string_to_local(utc_str, client_tz_str):
    """
    Convert a naive UTC datetime string to client's local timezone datetime.
    """
    dt_naive = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
    dt_utc = pytz.UTC.localize(dt_naive)
    client_tz = pytz.timezone(client_tz_str)
    return dt_utc.astimezone(client_tz)



from dateutil import parser

def enrich_event_with_local_timestamp(event_data, tz_name='UTC'):
    """
    Converts the existing UTC timestamp in event_data['timestamp'] to the client's local timezone,
    and stores it as event_data['timestamp'] formatted as '%Y-%m-%d %H:%M:%S'.
    Handles missing, empty, or ISO 8601 timestamps.
    
    tz_name: string like 'Europe/Budapest' or 'UTC', pre-fetched to avoid DB queries per event.
    """
    try:
        if not event_data:
            return

        # Extract timestamp
        utc_ts_str = event_data.get('timestamp')
        if not utc_ts_str or utc_ts_str.strip() == "":
            print(f"[WARN] Missing 'timestamp' in event_data. Skipping conversion.")
            return  # skip conversion

        # Parse ISO 8601 timestamp
        try:
            utc_dt = parser.isoparse(utc_ts_str)  # handles 'Z' and offsets automatically
        except Exception as parse_err:
            print(f"[WARN] Failed to parse timestamp '{utc_ts_str}': {parse_err}")
            return

        # Convert to local timezone
        local_dt = convert_utc_string_to_local(utc_dt, tz_name)
        event_data['timestamp'] = local_dt.strftime('%Y-%m-%d %H:%M:%S')
       
    except Exception as e:
        print(f"[WARN] Failed to convert timestamp: {e}")
        # leave original timestamp untouched




import AirplanemodeActiveRounded from '@mui/icons-material/AirplanemodeActiveRounded';
import type { SvgIconProps } from '@mui/material';
import { AirCanadaLogo } from '~/assets/AirCanadaLogo';
import { AirFranceLogo } from '~/assets/AirFranceLogo';
import { AlaskaLogo } from '~/assets/AlaskaLogo';
import { AllegiantLogo } from '~/assets/AllegiantLogo';
import { AmericanLogo } from '~/assets/AmericanLogo';
import { BritishAirLogo } from '~/assets/BritishAirLogo';
import { DeltaLogo } from '~/assets/DeltaLogo';
import { FrontierLogo } from '~/assets/FrontierLogo';
import { HawaiianLogo } from '~/assets/HawaiianLogo';
import { JetBlueLogo } from '~/assets/JetBlueLogo';
import { QantasLogo } from '~/assets/QantasLogo';
import { SouthwestLogo } from '~/assets/SouthwestLogo';
import { UnitedLogo } from '~/assets/UnitedLogo';
import { VirginLogo } from '~/assets/VirginLogo';

const _NONCONTINENTAL_ICAO_TO_IATA: Record<string, string> = {
  ANC: 'PANC',
  FAI: 'PAFA',
  JNU: 'PAJN',
  BET: 'PABT',
  KDK: 'PADQ',
  FBK: 'PAFB',
  AKN: 'PAKN',
  OME: 'PAOM',
  HOM: 'PAHO',
  ADK: 'PADK',
  HNL: 'PHNL',
  OGG: 'PHOG',
  KOA: 'PHKO',
  ITO: 'PHTO',
  SJU: 'TJSJ',
  BQN: 'TJBQ',
  PSE: 'TJPS',
  GUM: 'PGUM',
};

export const iataToIcao = (iata: string): string => {
  if (Object.keys(_NONCONTINENTAL_ICAO_TO_IATA).includes(iata))
    return _NONCONTINENTAL_ICAO_TO_IATA[iata];
  return `K${iata}`;
};

export const getCarrierLogo = (carrier: string, props?: SvgIconProps) => {
  switch (carrier) {
    case 'AA':
      return <AmericanLogo fontSize='small' {...props} />;
    case 'DL':
      return <DeltaLogo fontSize='small' {...props} />;
    case 'UA':
      return <UnitedLogo fontSize='small' {...props} />;
    case 'WN':
      return <SouthwestLogo fontSize='small' {...props} />;
    case 'AS':
      return <AlaskaLogo fontSize='small' {...props} />;
    case 'B6':
      return <JetBlueLogo fontSize='small' {...props} />;
    case 'F9':
      return <FrontierLogo fontSize='small' {...props} />;
    case 'HA':
      return <HawaiianLogo fontSize='small' {...props} />;
    case 'G4':
      return <AllegiantLogo fontSize='small' {...props} />;
    case 'VS':
      return <VirginLogo fontSize='small' {...props} />;
    case 'QF':
      return <QantasLogo fontSize='small' {...props} />;
    case 'AC':
      return <AirCanadaLogo fontSize='small' {...props} />;
    case 'AF':
      return <AirFranceLogo fontSize='small' {...props} />;
    case 'BA':
      return <BritishAirLogo fontSize='small' {...props} />;
    default:
      return (
        <AirplanemodeActiveRounded
          color='secondary'
          fontSize='small'
          {...props}
        />
      );
  }
};

// Derive from API ? Form error if flight is selected and carrier option doesn't match
export const CARRIERS = [
  { name: 'Aegean Airlines', code: 'A3' },
  { name: 'Aer Lingus', code: 'EI' },
  { name: 'Aeromexico', code: 'AM' },
  { name: 'Air Canada', code: 'AC' },
  { name: 'Air China', code: 'CA' },
  { name: 'Air France', code: 'AF' },
  { name: 'Air India', code: 'AI' },
  { name: 'Air New Zealand', code: 'NZ' },
  { name: 'Alaska Airlines', code: 'AS' },
  { name: 'All Nippon Airways', code: 'NH' },
  { name: 'American Airlines', code: 'AA' },
  { name: 'British Airways', code: 'BA' },
  { name: 'Cathay Pacific', code: 'CX' },
  { name: 'China Airlines', code: 'CI' },
  { name: 'Delta Air Lines', code: 'DL' },
  { name: 'Emirates', code: 'EK' },
  { name: 'Ethiopian Airlines', code: 'ET' },
  { name: 'EVA Air', code: 'BR' },
  { name: 'Finnair', code: 'AY' },
  { name: 'Garuda Indonesia', code: 'GA' },
  { name: 'Iberia', code: 'IB' },
  { name: 'Japan Airlines', code: 'JL' },
  { name: 'JetBlue Airways', code: 'B6' },
  { name: 'KLM Royal Dutch Airlines', code: 'KL' },
  { name: 'Korean Air', code: 'KE' },
  { name: 'Lufthansa', code: 'LH' },
  { name: 'Malaysia Airlines', code: 'MH' },
  { name: 'Qatar Airways', code: 'QR' },
  { name: 'Singapore Airlines', code: 'SQ' },
  { name: 'Southwest Airlines', code: 'WN' },
  { name: 'Thai Airways', code: 'TG' },
  { name: 'United Airlines', code: 'UA' },
  { name: 'Virgin Atlantic', code: 'VS' },
  { name: 'Fiji Airways', code: 'FJ' },
];

const CARRIER_ABRV_MAP: Record<string, string> = {
  AA: 'American Airlines',
  DL: 'Delta Air Lines',
  UA: 'United Airlines',
  WN: 'Southwest Airlines',
  AS: 'Alaska Airlines',
  B6: 'JetBlue Airways',
  NK: 'Spirit Airlines',
  F9: 'Frontier Airlines',
  HA: 'Hawaiian Airlines',
  G4: 'Allegiant Air',

  VS: 'Virgin Atlantic',
  QF: 'Qantas',
  AC: 'Air Canada',
  AF: 'Air France',
  BA: 'British Airways',

  // Major International Carriers
  CX: 'Cathay Pacific',
  EK: 'Emirates',
  ET: 'Ethiopian Airlines',
  IB: 'Iberia',
  JL: 'Japan Airlines',
  KL: 'KLM',
  LH: 'Lufthansa',
  QR: 'Qatar Airways',
  SQ: 'Singapore Airlines',
  TK: 'Turkish Airlines',
  LX: 'Swiss International Air Lines',
  EI: 'Aer Lingus',
  NZ: 'Air New Zealand',
  NH: 'All Nippon Airways',
  OZ: 'Asiana Airlines',
  OS: 'Austrian Airlines',
  AV: 'Avianca',
  EY: '联合航空 (Etihad Airways)',
  BR: 'EVA Air',
  AY: 'Finnair',
  KE: 'Korean Air',
  LA: 'LATAM Airlines',
  SK: 'Scandinavian Airlines (SAS)',
  TP: 'TAP Air Portugal',
};

export const carrierCodeToName = (code: string) => {
  return CARRIER_ABRV_MAP[code] || 'Unknown Carrier';
};

// only used for logging
export function getFlightCompositeId(origin: string, dest: string) {
  return `${origin}${dest}_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}`; // → "AA123_20240503"
}

export interface Airport {
  name: string;
  code: string;
  city: string;
}

export const AIRPORTS: Airport[] = [
  {
    code: 'ABE',
    name: 'Lehigh Valley International Airport',
    city: 'Allentown',
  },
  {
    code: 'ABI',
    name: 'Abilene Regional Airport',
    city: 'Abilene',
  },
  {
    code: 'ABQ',
    name: 'Albuquerque International Sunport Airport',
    city: 'Albuquerque',
  },
  {
    code: 'ABR',
    name: 'Aberdeen Regional Airport',
    city: 'Aberdeen',
  },
  {
    code: 'ABY',
    name: 'Southwest Georgia Regional Airport',
    city: 'Albany',
  },
  {
    code: 'ACK',
    name: 'Nantucket Memorial Airport',
    city: 'Nantucket',
  },
  {
    code: 'ACT',
    name: 'Waco Regional Airport',
    city: 'Waco',
  },
  {
    code: 'ACV',
    name: 'California Redwood Coast-Humboldt County Airport',
    city: 'Arcata',
  },
  {
    code: 'ACY',
    name: 'Atlantic City International Airport',
    city: 'Atlantic City',
  },
  {
    code: 'ADK',
    name: 'Adak Airport',
    city: 'Adak',
  },
  {
    code: 'ADQ',
    name: 'Kodiak Airport',
    city: 'Kodiak',
  },
  {
    code: 'AEX',
    name: 'Alexandria International Airport',
    city: 'Alexandria',
  },
  {
    code: 'AGS',
    name: 'Augusta Regional At Bush Field',
    city: 'Augusta',
  },
  {
    code: 'AKN',
    name: 'King Salmon Airport',
    city: 'King Salmon',
  },
  {
    code: 'ALB',
    name: 'Albany International Airport',
    city: 'Albany',
  },
  {
    code: 'ALO',
    name: 'Waterloo Regional Airport',
    city: 'Waterloo',
  },
  {
    code: 'AMA',
    name: 'Rick Husband Amarillo International Airport',
    city: 'Amarillo',
  },
  {
    code: 'ANC',
    name: 'Ted Stevens Anchorage International Airport',
    city: 'Anchorage',
  },
  {
    code: 'APN',
    name: 'Alpena County Regional Airport',
    city: 'Alpena',
  },
  {
    code: 'ASE',
    name: 'Aspen-Pitkin County/Sardy Field',
    city: 'Aspen',
  },
  {
    code: 'ATL',
    name: 'Hartsfield/Jackson Atlanta International Airport',
    city: 'Atlanta',
  },
  {
    code: 'ATW',
    name: 'Appleton International Airport',
    city: 'Appleton',
  },
  {
    code: 'AUS',
    name: 'Austin-Bergstrom International Airport',
    city: 'Austin',
  },
  {
    code: 'AVL',
    name: 'Asheville Regional Airport',
    city: 'Asheville',
  },
  {
    code: 'AVP',
    name: 'Wilkes-Barre/Scranton International Airport',
    city: 'Scranton',
  },
  {
    code: 'AZA',
    name: 'Mesa Gateway Airport',
    city: 'Mesa',
  },
  {
    code: 'AZO',
    name: 'Kalamazoo/Battle Creek International Airport',
    city: 'Kalamazoo',
  },
  {
    code: 'BDL',
    name: 'Bradley International Airport',
    city: 'Hartford',
  },
  {
    code: 'BET',
    name: 'Bethel Airport',
    city: 'Bethel',
  },
  {
    code: 'BFF',
    name: 'Scottsbluff/Western Nebraska Regional/Wm  B Heilig Field',
    city: 'Scottsbluff',
  },
  {
    code: 'BFL',
    name: 'Meadows Field',
    city: 'Bakersfield',
  },
  {
    code: 'BGM',
    name: 'Greater Binghamton/Edwin A Link Field',
    city: 'Binghamton',
  },
  {
    code: 'BGR',
    name: 'Bangor International Airport',
    city: 'Bangor',
  },
  {
    code: 'BHM',
    name: 'Birmingham-Shuttlesworth International Airport',
    city: 'Birmingham',
  },
  {
    code: 'BIH',
    name: 'Bishop Airport',
    city: 'Bishop',
  },
  {
    code: 'BIL',
    name: 'Billings Logan International Airport',
    city: 'Billings',
  },
  {
    code: 'BIS',
    name: 'Bismarck Municipal Airport',
    city: 'Bismarck',
  },
  {
    code: 'BJI',
    name: 'Bemidji Regional Airport',
    city: 'Bemidji',
  },
  {
    code: 'BLI',
    name: 'Bellingham International Airport',
    city: 'Bellingham',
  },
  {
    code: 'BLV',
    name: 'Scott Afb/Midamerica St Louis Airport',
    city: 'Belleville',
  },
  {
    code: 'BMI',
    name: 'Central Il Regional/Bloomington-Normal Airport',
    city: 'Bloomington',
  },
  {
    code: 'BNA',
    name: 'Nashville International Airport',
    city: 'Nashville',
  },
  {
    code: 'BOI',
    name: 'Boise Air Trml/Gowen Field',
    city: 'Boise',
  },
  {
    code: 'BOS',
    name: 'General Edward Lawrence Logan International Airport',
    city: 'Boston',
  },
  {
    code: 'BPT',
    name: 'Jack Brooks Regional Airport',
    city: 'Beaumont',
  },
  {
    code: 'BQN',
    name: 'Rafael Hernandez Airport',
    city: 'Aguadilla',
  },
  {
    code: 'BRD',
    name: 'Brainerd Lakes Regional Airport',
    city: 'Brainerd',
  },
  {
    code: 'BRO',
    name: 'Brownsville/South Padre Island International Airport',
    city: 'Brownsville',
  },
  {
    code: 'BRW',
    name: 'Wiley Post-Will Rogers Memorial Airport',
    city: 'Barrow',
  },
  {
    code: 'BTM',
    name: 'Bert Mooney Airport',
    city: 'Butte',
  },
  {
    code: 'BTR',
    name: 'Baton Rouge Metro, Ryan Field',
    city: 'Baton Rouge',
  },
  {
    code: 'BTV',
    name: 'Patrick Leahy Burlington International Airport',
    city: 'Burlington',
  },
  {
    code: 'BUF',
    name: 'Buffalo Niagara International Airport',
    city: 'Buffalo',
  },
  {
    code: 'BUR',
    name: 'Bob Hope Airport',
    city: 'Burbank',
  },
  {
    code: 'BWI',
    name: 'Baltimore/Washington International Thurgood Marshall Airport',
    city: 'Baltimore',
  },
  {
    code: 'BZN',
    name: 'Bozeman Yellowstone International Airport',
    city: 'Bozeman',
  },
  {
    code: 'CAE',
    name: 'Columbia Metro Airport',
    city: 'Columbia',
  },
  {
    code: 'CAK',
    name: 'Akron-Canton Regional Airport',
    city: 'Akron',
  },
  {
    code: 'CDC',
    name: 'Cedar City Regional Airport',
    city: 'Cedar City',
  },
  {
    code: 'CDV',
    name: 'Merle K (Mudhole) Smith Airport',
    city: 'Cordova',
  },
  {
    code: 'CHA',
    name: 'Lovell Field',
    city: 'Chattanooga',
  },
  {
    code: 'CHO',
    name: 'Charlottesville-Albemarle Airport',
    city: 'Charlottesville',
  },
  {
    code: 'CHS',
    name: 'Charleston Afb/International Airport',
    city: 'Charleston',
  },
  {
    code: 'CID',
    name: 'The Eastern Iowa Airport',
    city: 'Cedar Rapids',
  },
  {
    code: 'CIU',
    name: 'Chippewa County International Airport',
    city: 'Sault Ste. Marie',
  },
  {
    code: 'CKB',
    name: 'North Central West Virginia Airport',
    city: 'Clarksburg',
  },
  {
    code: 'CLD',
    name: 'Mc Clellan-Palomar Airport',
    city: 'Carlsbad',
  },
  {
    code: 'CLE',
    name: 'Cleveland-Hopkins International Airport',
    city: 'Cleveland',
  },
  {
    code: 'CLL',
    name: 'Easterwood Field',
    city: 'College Station',
  },
  {
    code: 'CLT',
    name: 'Charlotte/Douglas International Airport',
    city: 'Charlotte',
  },
  {
    code: 'CMH',
    name: 'John Glenn Columbus International Airport',
    city: 'Columbus',
  },
  {
    code: 'CMI',
    name: 'University Of Illinois/Willard Airport',
    city: 'Champaign',
  },
  {
    code: 'CMX',
    name: 'Houghton County Memorial Airport',
    city: 'Houghton',
  },
  {
    code: 'COD',
    name: 'Yellowstone Regional Airport',
    city: 'Cody',
  },
  {
    code: 'COS',
    name: 'City Of Colorado Springs Municipal Airport',
    city: 'Colorado Springs',
  },
  {
    code: 'COU',
    name: 'Columbia Regional Airport',
    city: 'Columbia',
  },
  {
    code: 'CPR',
    name: 'Casper/Natrona County International Airport',
    city: 'Casper',
  },
  {
    code: 'CRP',
    name: 'Corpus Christi International Airport',
    city: 'Corpus Christi',
  },
  {
    code: 'CRW',
    name: 'West Virginia International Yeager Airport',
    city: 'Charleston',
  },
  {
    code: 'CVG',
    name: 'Cincinnati/Northern Kentucky International Airport',
    city: 'Cincinnati',
  },
  {
    code: 'CWA',
    name: 'Central Wisconsin Airport',
    city: 'Wausau',
  },
  {
    code: 'CYS',
    name: 'Cheyenne Regional/Jerry Olson Field',
    city: 'Cheyenne',
  },
  {
    code: 'DAB',
    name: 'Daytona Beach International Airport',
    city: 'Daytona Beach',
  },
  {
    code: 'DAL',
    name: 'Dallas Love Field',
    city: 'Dallas',
  },
  {
    code: 'DAY',
    name: 'James M Cox Dayton International Airport',
    city: 'Dayton',
  },
  {
    code: 'DCA',
    name: 'Ronald Reagan Washington Ntl Airport',
    city: 'Washington',
  },
  {
    code: 'DDC',
    name: 'Dodge City Regional Airport',
    city: 'Dodge City',
  },
  {
    code: 'DEC',
    name: 'Decatur Airport',
    city: 'Decatur',
  },
  {
    code: 'DEN',
    name: 'Denver International Airport',
    city: 'Denver',
  },
  {
    code: 'DFW',
    name: 'Dallas-Fort Worth International Airport',
    city: 'Dallas',
  },
  {
    code: 'DHN',
    name: 'Dothan Regional Airport',
    city: 'Dothan',
  },
  {
    code: 'DIK',
    name: 'Dickinson/Theodore Roosevelt Regional Airport',
    city: 'Dickinson',
  },
  {
    code: 'DLG',
    name: 'Dillingham Airport',
    city: 'Dillingham',
  },
  {
    code: 'DLH',
    name: 'Duluth International Airport',
    city: 'Duluth',
  },
  {
    code: 'DRO',
    name: 'Durango-La Plata County Airport',
    city: 'Durango',
  },
  {
    code: 'DSM',
    name: 'Des Moines International Airport',
    city: 'Des Moines',
  },
  {
    code: 'DTW',
    name: 'Detroit Metro Wayne County Airport',
    city: 'Detroit',
  },
  {
    code: 'DVL',
    name: 'Devils Lake Regional Airport',
    city: 'Devils Lake',
  },
  {
    code: 'EAR',
    name: 'Kearney Regional Airport',
    city: 'Kearney',
  },
  {
    code: 'EAU',
    name: 'Chippewa Valley Regional Airport',
    city: 'Eau Claire',
  },
  {
    code: 'ECP',
    name: 'Northwest Florida Beaches International Airport',
    city: 'Panama City Beach',
  },
  {
    code: 'EGE',
    name: 'Eagle County Regional Airport',
    city: 'Eagle',
  },
  {
    code: 'EKO',
    name: 'Elko Regional Airport',
    city: 'Elko',
  },
  {
    code: 'ELM',
    name: 'Elmira/Corning Regional Airport',
    city: 'Elmira',
  },
  {
    code: 'ELP',
    name: 'El Paso International Airport',
    city: 'El Paso',
  },
  {
    code: 'ESC',
    name: 'Delta County Airport',
    city: 'Escanaba',
  },
  {
    code: 'EUG',
    name: 'Mahlon Sweet Field',
    city: 'Eugene',
  },
  {
    code: 'EVV',
    name: 'Evansville Regional Airport',
    city: 'Evansville',
  },
  {
    code: 'EWN',
    name: 'Coastal Carolina Regional Airport',
    city: 'New Bern',
  },
  {
    code: 'EWR',
    name: 'Newark Liberty International Airport',
    city: 'Newark',
  },
  {
    code: 'EYW',
    name: 'Key West International Airport',
    city: 'Key West',
  },
  {
    code: 'FAI',
    name: 'Fairbanks International Airport',
    city: 'Fairbanks',
  },
  {
    code: 'FAR',
    name: 'Hector International Airport',
    city: 'Fargo',
  },
  {
    code: 'FAT',
    name: 'Fresno Yosemite International Airport',
    city: 'Fresno',
  },
  {
    code: 'FAY',
    name: 'Fayetteville Regional/Grannis Field',
    city: 'Fayetteville',
  },
  {
    code: 'FCA',
    name: 'Glacier Park International Airport',
    city: 'Kalispell',
  },
  {
    code: 'FLG',
    name: 'Flagstaff Pulliam Airport',
    city: 'Flagstaff',
  },
  {
    code: 'FLL',
    name: 'Fort Lauderdale/Hollywood International Airport',
    city: 'Fort Lauderdale',
  },
  {
    code: 'FMN',
    name: 'Four Corners Regional Airport',
    city: 'Farmington',
  },
  {
    code: 'FNT',
    name: 'Bishop International Airport',
    city: 'Flint',
  },
  {
    code: 'FOD',
    name: 'Fort Dodge Regional Airport',
    city: 'Fort Dodge',
  },
  {
    code: 'FSD',
    name: 'Joe Foss Field',
    city: 'Sioux Falls',
  },
  {
    code: 'FSM',
    name: 'Fort Smith Regional Airport',
    city: 'Fort Smith',
  },
  {
    code: 'FWA',
    name: 'Fort Wayne International Airport',
    city: 'Fort Wayne',
  },
  {
    code: 'GCC',
    name: 'Northeast Wyoming Regional Airport',
    city: 'Gillette',
  },
  {
    code: 'GCK',
    name: 'Garden City Regional Airport',
    city: 'Garden City',
  },
  {
    code: 'GEG',
    name: 'Spokane International Airport',
    city: 'Spokane',
  },
  {
    code: 'GFK',
    name: 'Grand Forks International Airport',
    city: 'Grand Forks',
  },
  {
    code: 'GGG',
    name: 'East Texas Regional Airport',
    city: 'Longview',
  },
  {
    code: 'GJT',
    name: 'Grand Junction Regional Airport',
    city: 'Grand Junction',
  },
  {
    code: 'GNV',
    name: 'Gainesville Regional Airport',
    city: 'Gainesville',
  },
  {
    code: 'GPT',
    name: 'Gulfport-Biloxi International Airport',
    city: 'Gulfport',
  },
  {
    code: 'GRB',
    name: 'Green Bay/Austin Straubel International Airport',
    city: 'Green Bay',
  },
  {
    code: 'GRI',
    name: 'Central Nebraska Regional Airport',
    city: 'Grand Island',
  },
  {
    code: 'GRK',
    name: 'Robert Gray Army Air Field',
    city: 'Killeen',
  },
  {
    code: 'GRR',
    name: 'Gerald R Ford International Airport',
    city: 'Grand Rapids',
  },
  {
    code: 'GSO',
    name: 'Piedmont Triad International Airport',
    city: 'Greensboro',
  },
  {
    code: 'GSP',
    name: 'Greenville Spartanburg International Airport',
    city: 'Greenville',
  },
  {
    code: 'GST',
    name: 'Gustavus Airport',
    city: 'Gustavus',
  },
  {
    code: 'GTF',
    name: 'Great Falls International Airport',
    city: 'Great Falls',
  },
  {
    code: 'GTR',
    name: 'Golden Triangle Regional Airport',
    city: 'Columbus',
  },
  {
    code: 'GUC',
    name: 'Gunnison-Crested Butte Regional Airport',
    city: 'Gunnison',
  },
  {
    code: 'GUF',
    name: 'Gulf Shores International/Jack Edwards Field',
    city: 'Gulf Shores',
  },
  {
    code: 'GUM',
    name: 'Guam International Airport',
    city: 'Hagatna',
  },
  {
    code: 'HDN',
    name: 'Yampa Valley Airport',
    city: 'Hayden',
  },
  {
    code: 'HGR',
    name: 'Hagerstown Regional/Richard A Henson Field',
    city: 'Hagerstown',
  },
  {
    code: 'HHH',
    name: 'Hilton Head Airport',
    city: 'Hilton Head Island',
  },
  {
    code: 'HIB',
    name: 'Range Regional Airport',
    city: 'Hibbing',
  },
  {
    code: 'HLN',
    name: 'Helena Regional Airport',
    city: 'Helena',
  },
  {
    code: 'HNL',
    name: 'Daniel K Inouye International Airport',
    city: 'Honolulu',
  },
  {
    code: 'HOB',
    name: 'Lea County Regional Airport',
    city: 'Hobbs',
  },
  {
    code: 'HOU',
    name: 'William P Hobby Airport',
    city: 'Houston',
  },
  {
    code: 'HPN',
    name: 'Westchester County Airport',
    city: 'White Plains',
  },
  {
    code: 'HRL',
    name: 'Valley International Airport',
    city: 'Harlingen',
  },
  {
    code: 'HSV',
    name: 'Huntsville International-Carl T Jones Field',
    city: 'Huntsville',
  },
  {
    code: 'HTS',
    name: 'Tri-State/Milton J Ferguson Field',
    city: 'Huntington',
  },
  {
    code: 'HYA',
    name: 'Cape Cod Gateway Airport',
    city: 'Hyannis',
  },
  {
    code: 'HYS',
    name: 'Hays Regional Airport',
    city: 'Hays',
  },
  {
    code: 'IAD',
    name: 'Washington Dulles International Airport',
    city: 'Washington',
  },
  {
    code: 'IAG',
    name: 'Niagara Falls International Airport',
    city: 'Niagara Falls',
  },
  {
    code: 'IAH',
    name: 'George Bush Intcntl/Houston Airport',
    city: 'Houston',
  },
  {
    code: 'ICT',
    name: 'Wichita Dwight D Eisenhower Ntl Airport',
    city: 'Wichita',
  },
  {
    code: 'IDA',
    name: 'Idaho Falls Regional Airport',
    city: 'Idaho Falls',
  },
  {
    code: 'ILM',
    name: 'Wilmington International Airport',
    city: 'Wilmington',
  },
  {
    code: 'IMT',
    name: 'Ford Airport',
    city: 'Iron Mountain',
  },
  {
    code: 'IND',
    name: 'Indianapolis International Airport',
    city: 'Indianapolis',
  },
  {
    code: 'INL',
    name: 'Falls International/Einarson Field',
    city: 'International Falls',
  },
  {
    code: 'ISP',
    name: 'Long Island Mac Arthur Airport',
    city: 'Islip',
  },
  {
    code: 'ITO',
    name: 'Hilo International Airport',
    city: 'Hilo',
  },
  {
    code: 'JAC',
    name: 'Jackson Hole Airport',
    city: 'Jackson',
  },
  {
    code: 'JAN',
    name: 'Jackson-Medgar Wiley Evers International Airport',
    city: 'Jackson',
  },
  {
    code: 'JAX',
    name: 'Jacksonville International Airport',
    city: 'Jacksonville',
  },
  {
    code: 'JFK',
    name: 'John F Kennedy International Airport',
    city: 'New York',
  },
  {
    code: 'JLN',
    name: 'Joplin Regional Airport',
    city: 'Joplin',
  },
  {
    code: 'JMS',
    name: 'Jamestown Regional Airport',
    city: 'Jamestown',
  },
  {
    code: 'JNU',
    name: 'Juneau International Airport',
    city: 'Juneau',
  },
  {
    code: 'JST',
    name: 'John Murtha Johnstown/Cambria County Airport',
    city: 'Johnstown',
  },
  {
    code: 'KOA',
    name: 'Ellison Onizuka Kona International At Keahole Airport',
    city: 'Kailua-Kona',
  },
  {
    code: 'KTN',
    name: 'Ketchikan International Airport',
    city: 'Ketchikan',
  },
  {
    code: 'LAN',
    name: 'Capital Region International Airport',
    city: 'Lansing',
  },
  {
    code: 'LAR',
    name: 'Laramie Regional Airport',
    city: 'Laramie',
  },
  {
    code: 'LAS',
    name: 'Harry Reid International Airport',
    city: 'Las Vegas',
  },
  {
    code: 'LAW',
    name: 'Lawton-Fort Sill Regional Airport',
    city: 'Lawton',
  },
  {
    code: 'LAX',
    name: 'Los Angeles International Airport',
    city: 'Los Angeles',
  },
  {
    code: 'LBB',
    name: 'Lubbock Preston Smith International Airport',
    city: 'Lubbock',
  },
  {
    code: 'LBE',
    name: 'Arnold Palmer Regional Airport',
    city: 'Latrobe',
  },
  {
    code: 'LBF',
    name: 'North Platte Regional/Lee Bird Field',
    city: 'North Platte',
  },
  {
    code: 'LBL',
    name: 'Liberal Mid-America Regional Airport',
    city: 'Liberal',
  },
  {
    code: 'LCH',
    name: 'Lake Charles Regional Airport',
    city: 'Lake Charles',
  },
  {
    code: 'LCK',
    name: 'Rickenbacker International Airport',
    city: 'Columbus',
  },
  {
    code: 'LEX',
    name: 'Blue Grass Airport',
    city: 'Lexington',
  },
  {
    code: 'LFT',
    name: 'Lafayette Regional/Paul Fournet Field',
    city: 'Lafayette',
  },
  {
    code: 'LGA',
    name: 'Laguardia Airport',
    city: 'New York',
  },
  {
    code: 'LGB',
    name: 'Long Beach (Daugherty Field) Airport',
    city: 'Long Beach',
  },
  {
    code: 'LIH',
    name: 'Lihue Airport',
    city: 'Lihue',
  },
  {
    code: 'LIT',
    name: 'Bill And Hillary Clinton Ntl/Adams Field',
    city: 'Little Rock',
  },
  {
    code: 'LNK',
    name: 'Lincoln Airport',
    city: 'Lincoln',
  },
  {
    code: 'LRD',
    name: 'Laredo International Airport',
    city: 'Laredo',
  },
  {
    code: 'LSE',
    name: 'La Crosse Regional Airport',
    city: 'La Crosse',
  },
  {
    code: 'LWS',
    name: 'Lewiston/Nez Perce County Airport',
    city: 'Lewiston',
  },
  {
    code: 'MAF',
    name: 'Midland International Air And Space Port Airport',
    city: 'Midland',
  },
  {
    code: 'MBS',
    name: 'Mbs International Airport',
    city: 'Saginaw',
  },
  {
    code: 'MCI',
    name: 'Kansas City International Airport',
    city: 'Kansas City',
  },
  {
    code: 'MCO',
    name: 'Orlando International Airport',
    city: 'Orlando',
  },
  {
    code: 'MCW',
    name: 'Mason City Municipal Airport',
    city: 'Mason City',
  },
  {
    code: 'MDT',
    name: 'Harrisburg International Airport',
    city: 'Harrisburg',
  },
  {
    code: 'MDW',
    name: 'Chicago Midway International Airport',
    city: 'Chicago',
  },
  {
    code: 'MEI',
    name: 'Key Field',
    city: 'Meridian',
  },
  {
    code: 'MEM',
    name: 'Frederick W Smith International Airport',
    city: 'Memphis',
  },
  {
    code: 'MFE',
    name: 'Mc Allen International Airport',
    city: 'McAllen',
  },
  {
    code: 'MFR',
    name: 'Rogue Valley International/Medford Airport',
    city: 'Medford',
  },
  {
    code: 'MGM',
    name: 'Montgomery Regional (Dannelly Field) Airport',
    city: 'Montgomery',
  },
  {
    code: 'MGW',
    name: 'Morgantown Municipal/Walter L Bill Hart Field',
    city: 'Morgantown',
  },
  {
    code: 'MHK',
    name: 'Manhattan Regional Airport',
    city: 'Manhattan',
  },
  {
    code: 'MHT',
    name: 'Manchester Boston Regional Airport',
    city: 'Manchester',
  },
  {
    code: 'MIA',
    name: 'Miami International Airport',
    city: 'Miami',
  },
  {
    code: 'MKE',
    name: 'General Mitchell International Airport',
    city: 'Milwaukee',
  },
  {
    code: 'MLB',
    name: 'Melbourne Orlando International Airport',
    city: 'Melbourne',
  },
  {
    code: 'MLI',
    name: 'Quad Cities International Airport',
    city: 'Moline',
  },
  {
    code: 'MLU',
    name: 'Monroe Regional Airport',
    city: 'Monroe',
  },
  {
    code: 'MOB',
    name: 'Mobile Regional Airport',
    city: 'Mobile',
  },
  {
    code: 'MOT',
    name: 'Minot International Airport',
    city: 'Minot',
  },
  {
    code: 'MQT',
    name: 'Marquette/Sawyer Regional Airport',
    city: 'Marquette',
  },
  {
    code: 'MRY',
    name: 'Monterey Regional Airport',
    city: 'Monterey',
  },
  {
    code: 'MSN',
    name: 'Dane County Regional/Truax Field',
    city: 'Madison',
  },
  {
    code: 'MSO',
    name: 'Missoula Montana Airport',
    city: 'Missoula',
  },
  {
    code: 'MSP',
    name: 'Minneapolis-St Paul International/Wold-Chamberlain Airport',
    city: 'Minneapolis',
  },
  {
    code: 'MSY',
    name: 'Louis Armstrong New Orleans International Airport',
    city: 'New Orleans',
  },
  {
    code: 'MTJ',
    name: 'Montrose Regional Airport',
    city: 'Montrose',
  },
  {
    code: 'MVY',
    name: "Martha's Vineyard Airport",
    city: "Martha's Vineyard",
  },
  {
    code: 'MYR',
    name: 'Myrtle Beach International Airport',
    city: 'Myrtle Beach',
  },
  {
    code: 'OAJ',
    name: 'Albert J Ellis Airport',
    city: 'Jacksonville',
  },
  {
    code: 'OAK',
    name: 'Oakland San Francisco Bay Airport',
    city: 'Oakland',
  },
  {
    code: 'OGG',
    name: 'Kahului Airport',
    city: 'Kahului',
  },
  {
    code: 'OKC',
    name: 'Okc Will Rogers International Airport',
    city: 'Oklahoma City',
  },
  {
    code: 'OMA',
    name: 'Eppley Airfield',
    city: 'Omaha',
  },
  {
    code: 'OME',
    name: 'Nome Airport',
    city: 'Nome',
  },
  {
    code: 'ONT',
    name: 'Ontario International Airport',
    city: 'Ontario',
  },
  {
    code: 'ORD',
    name: "Chicago O'Hare International Airport",
    city: 'Chicago',
  },
  {
    code: 'ORF',
    name: 'Norfolk International Airport',
    city: 'Norfolk',
  },
  {
    code: 'ORH',
    name: 'Worcester Regional Airport',
    city: 'Worcester',
  },
  {
    code: 'OTH',
    name: 'Southwest Oregon Regional Airport',
    city: 'North Bend',
  },
  {
    code: 'OTZ',
    name: 'Ralph Wien Memorial Airport',
    city: 'Kotzebue',
  },
  {
    code: 'PAE',
    name: 'Seattle Paine Field International Airport',
    city: 'Everett',
  },
  {
    code: 'PBG',
    name: 'Plattsburgh International Airport',
    city: 'Plattsburgh',
  },
  {
    code: 'PBI',
    name: 'Palm Beach International Airport',
    city: 'West Palm Beach',
  },
  {
    code: 'PDX',
    name: 'Portland International Airport',
    city: 'Portland',
  },
  {
    code: 'PGD',
    name: 'Punta Gorda Airport',
    city: 'Punta Gorda',
  },
  {
    code: 'PHL',
    name: 'Philadelphia International Airport',
    city: 'Philadelphia',
  },
  {
    code: 'PHX',
    name: 'Phoenix Sky Harbor International Airport',
    city: 'Phoenix',
  },
  {
    code: 'PIA',
    name: 'General Downing - Peoria International Airport',
    city: 'Peoria',
  },
  {
    code: 'PIB',
    name: 'Hattiesburg/Laurel Regional Airport',
    city: 'Hattiesburg',
  },
  {
    code: 'PIE',
    name: 'St Pete-Clearwater International Airport',
    city: 'St. Petersburg',
  },
  {
    code: 'PIH',
    name: 'Pocatello Regional Airport',
    city: 'Pocatello',
  },
  {
    code: 'PIT',
    name: 'Pittsburgh International Airport',
    city: 'Pittsburgh',
  },
  {
    code: 'PLN',
    name: 'Pellston Regional/Emmet County Airport',
    city: 'Pellston',
  },
  {
    code: 'PNS',
    name: 'Pensacola International Airport',
    city: 'Pensacola',
  },
  {
    code: 'PPG',
    name: 'Pago Pago International Airport',
    city: 'Pago Pago',
  },
  {
    code: 'PQI',
    name: 'Presque Isle International Airport',
    city: 'Presque Isle',
  },
  {
    code: 'PRC',
    name: 'Prescott Regional/Ernest A Love Field',
    city: 'Prescott',
  },
  {
    code: 'PSC',
    name: 'Tri-Cities Airport',
    city: 'Pasco',
  },
  {
    code: 'PSE',
    name: 'Mercedita Airport',
    city: 'Ponce',
  },
  {
    code: 'PSG',
    name: 'Petersburg James A Johnson Airport',
    city: 'Petersburg',
  },
  {
    code: 'PSM',
    name: 'Portsmouth International At Pease Airport',
    city: 'Portsmouth',
  },
  {
    code: 'PSP',
    name: 'Palm Springs International Airport',
    city: 'Palm Springs',
  },
  {
    code: 'PVD',
    name: 'Rhode Island Tf Green International Airport',
    city: 'Providence',
  },
  {
    code: 'PVU',
    name: 'Provo Municipal Airport',
    city: 'Provo',
  },
  {
    code: 'PWM',
    name: 'Portland International Jetport Airport',
    city: 'Portland',
  },
  {
    code: 'RAP',
    name: 'Rapid City Regional Airport',
    city: 'Rapid City',
  },
  {
    code: 'RDD',
    name: 'Redding Regional Airport',
    city: 'Redding',
  },
  {
    code: 'RDM',
    name: 'Roberts Field',
    city: 'Redmond',
  },
  {
    code: 'RDU',
    name: 'Raleigh-Durham International Airport',
    city: 'Raleigh',
  },
  {
    code: 'RFD',
    name: 'Chicago/Rockford International Airport',
    city: 'Rockford',
  },
  {
    code: 'RHI',
    name: 'Rhinelander/Oneida County Airport',
    city: 'Rhinelander',
  },
  {
    code: 'RIC',
    name: 'Richmond International Airport',
    city: 'Richmond',
  },
  {
    code: 'RIW',
    name: 'Central Wyoming Regional Airport',
    city: 'Riverton',
  },
  {
    code: 'RKS',
    name: 'Southwest Wyoming Regional Airport',
    city: 'Rock Springs',
  },
  {
    code: 'RNO',
    name: 'Reno/Tahoe International Airport',
    city: 'Reno',
  },
  {
    code: 'ROA',
    name: 'Roanoke/Blacksburg Regional (Woodrum Field) Airport',
    city: 'Roanoke',
  },
  {
    code: 'ROC',
    name: 'Frederick Douglass/Greater Rochester International Airport',
    city: 'Rochester',
  },
  {
    code: 'ROW',
    name: 'Roswell Air Center Airport',
    city: 'Roswell',
  },
  {
    code: 'RST',
    name: 'Rochester International Airport',
    city: 'Rochester',
  },
  {
    code: 'RSW',
    name: 'Southwest Florida International Airport',
    city: 'Fort Myers',
  },
  {
    code: 'SAF',
    name: 'Santa Fe Regional Airport',
    city: 'Santa Fe',
  },
  {
    code: 'SAN',
    name: 'San Diego International Airport',
    city: 'San Diego',
  },
  {
    code: 'SAT',
    name: 'San Antonio International Airport',
    city: 'San Antonio',
  },
  {
    code: 'SAV',
    name: 'Savannah/Hilton Head International Airport',
    city: 'Savannah',
  },
  {
    code: 'SBA',
    name: 'Santa Barbara Municipal Airport',
    city: 'Santa Barbara',
  },
  {
    code: 'SBN',
    name: 'South Bend International Airport',
    city: 'South Bend',
  },
  {
    code: 'SBP',
    name: 'San Luis Obispo County Regional Airport',
    city: 'San Luis Obispo',
  },
  {
    code: 'SCC',
    name: 'Deadhorse Airport',
    city: 'Deadhorse',
  },
  {
    code: 'SCE',
    name: 'State College Regional Airport',
    city: 'State College',
  },
  {
    code: 'SCK',
    name: 'Stockton Metro Airport',
    city: 'Stockton',
  },
  {
    code: 'SDF',
    name: 'Louisville Muhammad Ali International Airport',
    city: 'Louisville',
  },
  {
    code: 'SEA',
    name: 'Seattle-Tacoma International Airport',
    city: 'Seattle',
  },
  {
    code: 'SFB',
    name: 'Orlando Sanford International Airport',
    city: 'Sanford',
  },
  {
    code: 'SFO',
    name: 'San Francisco International Airport',
    city: 'San Francisco',
  },
  {
    code: 'SGF',
    name: 'Springfield-Branson Ntl Airport',
    city: 'Springfield',
  },
  {
    code: 'SGU',
    name: 'St George Regional Airport',
    city: 'St. George',
  },
  {
    code: 'SHR',
    name: 'Sheridan County Airport',
    city: 'Sheridan',
  },
  {
    code: 'SHV',
    name: 'Shreveport Regional Airport',
    city: 'Shreveport',
  },
  {
    code: 'SIT',
    name: 'Sitka Rocky Gutierrez Airport',
    city: 'Sitka',
  },
  {
    code: 'SJC',
    name: 'Norman Y Mineta San Jose International Airport',
    city: 'San Jose',
  },
  {
    code: 'SJT',
    name: 'San Angelo Regional/Mathis Field',
    city: 'San Angelo',
  },
  {
    code: 'SJU',
    name: 'Luis Munoz Marin International Airport',
    city: 'San Juan',
  },
  {
    code: 'SLC',
    name: 'Salt Lake City International Airport',
    city: 'Salt Lake City',
  },
  {
    code: 'SLN',
    name: 'Salina Regional Airport',
    city: 'Salina',
  },
  {
    code: 'SMF',
    name: 'Sacramento International Airport',
    city: 'Sacramento',
  },
  {
    code: 'SMX',
    name: 'Santa Maria Pub/Capt G Allan Hancock Field',
    city: 'Santa Maria',
  },
  {
    code: 'SNA',
    name: 'John Wayne/Orange County Airport',
    city: 'Santa Ana',
  },
  {
    code: 'SPI',
    name: 'Abraham Lincoln Capital Airport',
    city: 'Springfield',
  },
  {
    code: 'SPS',
    name: 'Sheppard Afb/Wichita Falls Municipal Airport',
    city: 'Wichita Falls',
  },
  {
    code: 'SRQ',
    name: 'Sarasota/Bradenton International Airport',
    city: 'Sarasota',
  },
  {
    code: 'STC',
    name: 'St Cloud Regional Airport',
    city: 'St. Cloud',
  },
  {
    code: 'STL',
    name: 'St Louis Lambert International Airport',
    city: 'St. Louis',
  },
  {
    code: 'STS',
    name: 'Charles M Schulz/Sonoma County Airport',
    city: 'Santa Rosa',
  },
  {
    code: 'STT',
    name: 'Cyril E King Airport',
    city: 'Charlotte Amalie',
  },
  {
    code: 'STX',
    name: 'Henry E Rohlsen Airport',
    city: 'Christiansted',
  },
  {
    code: 'SUN',
    name: 'Friedman Memorial Airport',
    city: 'Hailey',
  },
  {
    code: 'SUX',
    name: 'Sioux Gateway/Brig General Bud Day Field',
    city: 'Sioux City',
  },
  {
    code: 'SWF',
    name: 'New York Stewart International Airport',
    city: 'Newburgh',
  },
  {
    code: 'SWO',
    name: 'Stillwater Regional Airport',
    city: 'Stillwater',
  },
  {
    code: 'SYR',
    name: 'Syracuse Hancock International Airport',
    city: 'Syracuse',
  },
  {
    code: 'TLH',
    name: 'Tallahassee International Airport',
    city: 'Tallahassee',
  },
  {
    code: 'TOL',
    name: 'Eugene F Kranz Toledo Express Airport',
    city: 'Toledo',
  },
  {
    code: 'TPA',
    name: 'Tampa International Airport',
    city: 'Tampa',
  },
  {
    code: 'TRI',
    name: 'Tri-Cities Airport',
    city: 'Bristol',
  },
  {
    code: 'TTN',
    name: 'Trenton Mercer Airport',
    city: 'Trenton',
  },
  {
    code: 'TUL',
    name: 'Tulsa International Airport',
    city: 'Tulsa',
  },
  {
    code: 'TUS',
    name: 'Tucson International Airport',
    city: 'Tucson',
  },
  {
    code: 'TVC',
    name: 'Cherry Capital Airport',
    city: 'Traverse City',
  },
  {
    code: 'TWF',
    name: 'Joslin Field/Magic Valley Regional Airport',
    city: 'Twin Falls',
  },
  {
    code: 'TXK',
    name: 'Texarkana Regional-Webb Field',
    city: 'Texarkana',
  },
  {
    code: 'TYR',
    name: 'Tyler Pounds Regional Airport',
    city: 'Tyler',
  },
  {
    code: 'TYS',
    name: 'Mc Ghee Tyson Airport',
    city: 'Knoxville',
  },
  {
    code: 'USA',
    name: 'Concord-Padgett Regional Airport',
    city: 'Concord',
  },
  {
    code: 'VCT',
    name: 'Victoria Regional Airport',
    city: 'Victoria',
  },
  {
    code: 'VPS',
    name: 'Eglin Afb/Destin-Ft Walton Beach Airport',
    city: 'Fort Walton Beach',
  },
  {
    code: 'WRG',
    name: 'Wrangell Airport',
    city: 'Wrangell',
  },
  {
    code: 'WYS',
    name: 'Yellowstone Airport',
    city: 'West Yellowstone',
  },
  {
    code: 'XNA',
    name: 'Northwest Arkansas Ntl Airport',
    city: 'Bentonville',
  },
  {
    code: 'XWA',
    name: 'Williston Basin International Airport',
    city: 'Williston',
  },
  {
    code: 'YAK',
    name: 'Yakutat Airport',
    city: 'Yakutat',
  },
  {
    code: 'YUM',
    name: 'Yuma Mcas/Yuma International Airport',
    city: 'Yuma',
  },
];

export const getAirportCity = (code: string) => {
  return AIRPORTS.find((a) => a.code === code)?.city || '';
};
